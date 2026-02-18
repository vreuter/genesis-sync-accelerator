{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections #-}
{-# OPTIONS_GHC -Wno-orphans #-}

module Test.GenesisSyncAccelerator.Storage (tests) where

import Control.Monad (foldM, unless)
import System.Directory (doesFileExist)
import System.FilePath ((</>))
import qualified System.IO.Temp as Temp

import qualified Data.Text as Text
import GenesisSyncAccelerator.OnDemand (deleteChunkFiles)
import GenesisSyncAccelerator.RemoteStorage (FileType (..), getFileName, toSuffix)
import Ouroboros.Consensus.Storage.ImmutableDB.Chunks.Internal
  ( ChunkNo (..)
  )
import Ouroboros.Consensus.Storage.ImmutableDB.Impl.Util
  ( fsPathChunkFile
  , fsPathPrimaryIndexFile
  , fsPathSecondaryIndexFile
  , parseDBFile
  , renderFile
  )
import System.FS.API.Types (FsPath, MountPoint (MountPoint), fsPathToList)
import System.FS.IO (ioHasFS)
import Test.QuickCheck
import Test.QuickCheck.Instances ()
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.QuickCheck (testProperty)

import Test.GenesisSyncAccelerator.Orphans ()
import Test.GenesisSyncAccelerator.Utilities
  ( allFileTypes
  , genSeveralChunkNumbers
  , getAllFilenamesForChunk
  , getCurrentFilenamesForChunk
  )

{-# ANN module "HLint: ignore Use camelCase" #-}

----------------------------- Generators and properties -----------------------------

instance Arbitrary FileType where
  arbitrary =
    elements
      [ ChunkFile
      , PrimaryIndexFile
      , SecondaryIndexFile
      , EpochFile
      ]

genSuffixAndValidityFlag :: Gen (Text.Text, Bool)
genSuffixAndValidityFlag =
  let validSuffixes = map toSuffix allFileTypes
   in frequency
        [ (3, (,True) <$> elements validSuffixes)
        , (7, (,False) <$> arbitrary `suchThat` (`notElem` validSuffixes))
        ]

gen_target_chunk_setup_and_expectation_for_deleteChunkFiles ::
  Gen (ChunkNo, FilesSetup, FilesExpectation)
gen_target_chunk_setup_and_expectation_for_deleteChunkFiles = do
  -- We need at least one target chunk and one additional chunk for the test to be meaningful.
  -- Combine a limited domain with shuffling so that it's relatively cheap to ensure distinct elements.
  chunkNumbers <- genSeveralChunkNumbers
  -- We pick a target chunk number (for file deletion) and then construct expectations around it.
  targetChunk <- elements chunkNumbers
  let otherChunks = filter (/= targetChunk) chunkNumbers
      shouldNotExist = getCurrentFilenamesForChunk targetChunk
      shouldExist = otherChunks >>= getCurrentFilenamesForChunk
      setup =
        FilesSetup
          { filesToCreate = shouldExist ++ shouldNotExist
          , filesToNotCreate = []
          }
      expectation =
        FilesExpectation
          { filesThatShouldExist = shouldExist
          , filesThatShouldNotExist = shouldNotExist
          }
  pure (targetChunk, setup, expectation)

buildPropFsPathFileTypeIsCorrect :: FileType -> (ChunkNo -> FsPath) -> Property
buildPropFsPathFileTypeIsCorrect ft getFsPath =
  forAll arbitrary $ \cn ->
    let fsPathChunks = fsPathToList (getFsPath cn)
        observed = traverse (parseDBFile . Text.unpack) fsPathChunks
        expected = Just [getParseDBFileExpectation ft cn]
     in observed === expected

prop_fsPathChunkFileIsCorrect :: Property
prop_fsPathChunkFileIsCorrect =
  buildPropFsPathFileTypeIsCorrect ChunkFile fsPathChunkFile

prop_fsPathPrimaryIndexFileIsCorrect :: Property
prop_fsPathPrimaryIndexFileIsCorrect =
  buildPropFsPathFileTypeIsCorrect PrimaryIndexFile fsPathPrimaryIndexFile

prop_fsPathSecondaryIndexFileIsCorrect :: Property
prop_fsPathSecondaryIndexFileIsCorrect =
  buildPropFsPathFileTypeIsCorrect SecondaryIndexFile fsPathSecondaryIndexFile

prop_getFileName_is_compatible_with_parseDBFile_through_toSuffix :: FileType -> ChunkNo -> Property
prop_getFileName_is_compatible_with_parseDBFile_through_toSuffix ft cn =
  let fn = Text.unpack $ getFileName ft cn
   in parseDBFile fn === Just (getParseDBFileExpectation ft cn)

prop_getFileName_then_list__is_equivalent_to__renderFile_then_fsPathToList ::
  FileType -> ChunkNo -> Property
prop_getFileName_then_list__is_equivalent_to__renderFile_then_fsPathToList ft cn =
  [getFileName ft cn] === fsPathToList (renderFile (toSuffix ft) cn)

prop_deleteChunkFiles_handles_missing_files :: Property
prop_deleteChunkFiles_handles_missing_files =
  forAll gen_ChunkNo_and_FilesSetup $ \(chunkNo, fnsSetup) ->
    ioProperty $
      Temp.withSystemTempDirectory tmpSub $ \tmp -> do
        fnsSetup' <- setupFiles tmp fnsSetup
        deleteChunkFiles (ioHasFS $ MountPoint tmp) chunkNo
        -- No file should exist after deletion, regardless of its existence before deletion.
        existences <- mapM doesFileExist (filesToCreate fnsSetup' ++ filesToNotCreate fnsSetup')
        pure $ not $ or existences
 where
  gen_ChunkNo_and_FilesSetup = do
    chunkNo <- arbitrary
    let fns = getCurrentFilenamesForChunk chunkNo
    toCreate <- sublistOf fns
    let toNotCreate = filter (`notElem` toCreate) fns
        setup = FilesSetup toCreate toNotCreate
    pure (chunkNo, setup)

prop_deleteChunkFiles_is_completely_sensitive_to_chunk_number :: Property
prop_deleteChunkFiles_is_completely_sensitive_to_chunk_number =
  forAll gen_target_chunk_setup_and_expectation_for_deleteChunkFiles $ \(targetChunk, setup, FilesExpectation{..}) ->
    ioProperty $
      Temp.withSystemTempDirectory tmpSub $ \tmp -> do
        _ <- setupFiles tmp setup
        deleteChunks tmp targetChunk
        -- Expected deletions should have all been done.
        and <$> mapM (fmap not . doesFileExist . (tmp </>)) filesThatShouldNotExist

prop_deleteChunkFiles_is_completely_specific_to_chunk_number :: Property
prop_deleteChunkFiles_is_completely_specific_to_chunk_number =
  forAll gen_target_chunk_setup_and_expectation_for_deleteChunkFiles $ \(targetChunk, setup, FilesExpectation{..}) ->
    ioProperty $
      Temp.withSystemTempDirectory tmpSub $ \tmp -> do
        _ <- setupFiles tmp setup
        deleteChunks tmp targetChunk
        -- Expected non-deletions should have all been preserved.
        and <$> mapM (doesFileExist . (tmp </>)) filesThatShouldExist

prop_deleteChunkFiles_ignores_EpochFile :: ChunkNo -> Property
prop_deleteChunkFiles_ignores_EpochFile chunkNo =
  ioProperty $
    Temp.withSystemTempDirectory tmpSub $ \tmp -> do
      let fns = getAllFilenamesForChunk chunkNo
      _ <- setupFiles tmp (FilesSetup fns [])
      deleteChunks tmp chunkNo
      doesFileExist (tmp </> Text.unpack (getFileName EpochFile chunkNo))

----------------------------- Helper functions and types -----------------------------
tmpSub :: String
tmpSub = "storage-test"

deleteChunks :: FilePath -> ChunkNo -> IO ()
deleteChunks folder = deleteChunkFiles (ioHasFS $ MountPoint folder)

getParseDBFileExpectation :: FileType -> ChunkNo -> (String, ChunkNo)
getParseDBFileExpectation ft cn = (Text.unpack $ toSuffix ft, cn)

setupFiles :: FilePath -> FilesSetup -> IO FilesSetup
setupFiles tmp FilesSetup{..} = do
  let makeFile fn = writeFile fp "filler text" >> pure fp
       where
        fp = tmp </> fn
      nonExtantPaths = map (tmp </>) filesToNotCreate
  extantPaths <- foldM (\acc fn -> (: acc) <$> makeFile fn) [] filesToCreate
  pretestPositive <- and <$> mapM doesFileExist extantPaths
  pretestNegative <- and <$> mapM (fmap not . doesFileExist) nonExtantPaths
  unless pretestPositive $
    error "Failed to set up test: some files that were supposed to be created were not."
  unless pretestNegative $
    error "Failed to set up test: some files that were supposed to not be created were."
  pure $ FilesSetup extantPaths nonExtantPaths

data FilesExpectation = FilesExpectation
  { filesThatShouldExist :: [String]
  , filesThatShouldNotExist :: [String]
  }
  deriving (Eq, Show)

data FilesSetup = FilesSetup
  { filesToCreate :: [String]
  , filesToNotCreate :: [String]
  }
  deriving (Eq, Show)

----------------------------- Property aggregation -----------------------------
tests :: TestTree
tests =
  testGroup
    "Storage-related utilities"
    [ testProperty
        "getFileName is compatible with parseDBFile through toSuffix"
        prop_getFileName_is_compatible_with_parseDBFile_through_toSuffix
    , testProperty
        "fsPathChunkFile parses as ChunkFile"
        prop_fsPathChunkFileIsCorrect
    , testProperty
        "fsPathPrimaryIndexFile parses as PrimaryIndexFile"
        prop_fsPathPrimaryIndexFileIsCorrect
    , testProperty
        "fsPathSecondaryIndexFile parses as SecondaryIndexFile"
        prop_fsPathSecondaryIndexFileIsCorrect
    , testProperty
        "getFileName then list is equivalent to renderFile then fsPathToList"
        prop_getFileName_then_list__is_equivalent_to__renderFile_then_fsPathToList
    , testProperty
        "deleteChunkFiles handles missing files gracefully"
        prop_deleteChunkFiles_handles_missing_files
    , testProperty
        "deleteChunkFiles is completely sensitive to the chunk number"
        prop_deleteChunkFiles_is_completely_sensitive_to_chunk_number
    , testProperty
        "deleteChunkFiles is completely specific to the chunk number"
        prop_deleteChunkFiles_is_completely_specific_to_chunk_number
    , testProperty
        "deleteChunkFiles ignores EpochFile"
        prop_deleteChunkFiles_ignores_EpochFile
    ]
