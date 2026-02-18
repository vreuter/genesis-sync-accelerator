{-# LANGUAGE PackageImports #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE TupleSections #-}

module Test.GenesisSyncAccelerator.Download (tests) where

import Control.Monad (filterM, foldM, forM_, unless)
import qualified Data.ByteString.Lazy as LBS
import qualified Data.List as List
import qualified Data.Map as Map
import Data.Maybe (catMaybes, fromMaybe)
import Data.Ord (comparing)
import qualified Data.Set as Set
import qualified Data.Text as Text
import qualified Data.Text.IO as TIO
import "contra-tracer" Control.Tracer (Tracer (..), nullTracer, showTracing, stdoutTracer)

import Network.Wai.Application.Static (defaultFileServerSettings, staticApp)
import Network.Wai.Handler.Warp (testWithApplication)
import Ouroboros.Consensus.Storage.ImmutableDB.Chunks.Internal
  ( ChunkNo (..)
  )
import System.Directory (createDirectoryIfMissing, doesFileExist, listDirectory)
import System.FilePath ((</>))
import System.IO (IOMode (ReadMode), hGetContents, withFile)
import qualified System.IO.Temp as Temp
import Test.QuickCheck
import Test.QuickCheck.Instances ()
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.QuickCheck (testProperty)

import Test.GenesisSyncAccelerator.Orphans ()
import Test.GenesisSyncAccelerator.Utilities
  ( allFileTypes
  , currentFileTypes
  , genSeveralChunkNumbers
  , getCurrentFilenamesForChunk
  )

import GenesisSyncAccelerator.RemoteStorage
  ( FileType (..)
  , RemoteStorageConfig (..)
  , TraceRemoteStorageEvent (..)
  , downloadChunk
  , getFileName
  )

{-# ANN module "HLint: ignore Use camelCase" #-}

----------------------------- Generators and properties -----------------------------

-- There should be exactly one file for each of the current file types, nothing more and nothing less.
prop_downloadChunk_downloads_files_as_expected_when_available :: Property
prop_downloadChunk_downloads_files_as_expected_when_available =
  forAll gen_target_chunk_and_file_kernels $ \(targetChunk, fileKernels) ->
    let eventualExpectedTargetFolderContents = getCurrentFilenamesForChunk targetChunk
     in ioProperty $
          Temp.withSystemTempDirectory tmpSub $ \tmp -> do
            setup@TestFolderSetup{..} <- setupFilesAndFolders tmp fileKernels
            initialTargetFolderContents <- listDirectory clientTmpdir
            case initialTargetFolderContents of
              [] -> do
                runDownloadChunk nullTracer setup targetChunk
                posthocTargetFolderContents <- listDirectory clientTmpdir
                pure $ List.sort posthocTargetFolderContents === List.sort eventualExpectedTargetFolderContents
              contents -> do
                error $ "The target folder is nonempty: " ++ show contents
 where
  gen_target_chunk_and_file_kernels :: Gen (ChunkNo, [FileKernel])
  gen_target_chunk_and_file_kernels = do
    allChunkNumbers <- genSeveralChunkNumbers
    targetChunk <- elements allChunkNumbers
    return (targetChunk, [(ServerSide, t, c) | t <- currentFileTypes, c <- allChunkNumbers])

-- For any requested file not available and not present locally, an error should be traced.
prop_downloadChunk_traces_errors_as_expected_when_files_are_unavailable :: Property
prop_downloadChunk_traces_errors_as_expected_when_files_are_unavailable =
  forAll gen_target_chunk_and_file_kernels_and_missing_file_types $ \(targetChunk, fileKernels, expectedMissingFileTypes) ->
    let expectedErrors = map (\t -> TraceDownloadError (nameFile t targetChunk) 404) expectedMissingFileTypes
     in ioProperty $
          Temp.withSystemTempDirectory tmpSub $ \tmp -> do
            let logfile = tmp </> "log.txt"
                tracer = tracerToFile logfile
            setup@TestFolderSetup{..} <- setupFilesAndFolders tmp fileKernels
            initialTargetFolderContents <- listDirectory clientTmpdir
            case initialTargetFolderContents of
              [] -> do
                runDownloadChunk tracer setup targetChunk
                logLines <- lines . Text.unpack <$> TIO.readFile logfile
                pure $
                  List.sort (filter ("TraceDownloadError" `List.isPrefixOf`) logLines)
                    === List.sort (map show expectedErrors)
              contents -> do
                error $ "The target folder is nonempty: " ++ show contents
 where
  gen_target_chunk_and_file_kernels_and_missing_file_types :: Gen (ChunkNo, [FileKernel], [FileType])
  gen_target_chunk_and_file_kernels_and_missing_file_types = do
    chunks <- genSeveralChunkNumbers
    targetChunk <- elements chunks
    missingFileTypes <- do
      n <- choose (1, length currentFileTypes - 1)
      take n <$> shuffle currentFileTypes
    let kernels =
          [ (ServerSide, ft, cn) | ft <- currentFileTypes, cn <- chunks, not $ ft `elem` missingFileTypes && cn == targetChunk
          ]
    return (targetChunk, kernels, missingFileTypes)

-- Missing files are downloaded while existing files are left alone.
prop_downloadChunk_correctly_handles_mixed_local_preexistence_of_files :: Property
prop_downloadChunk_correctly_handles_mixed_local_preexistence_of_files =
  forAll gen_target_chunk_and_file_kernels $ \(targetChunk, fileKernels) ->
    let expectedPreDownloadFileNames =
          foldr
            (\(s, t, c) acc -> if s == ClientSide then Map.insert (t, c) (nameFile t c) acc else acc)
            mempty
            fileKernels
        allFileNames = List.sort $ getCurrentFilenamesForChunk targetChunk
     in ioProperty $
          Temp.withSystemTempDirectory tmpSub $ \tmp -> do
            setup@TestFolderSetup{..} <- setupFilesAndFolders tmp fileKernels
            preDownloadClientSideFileNames <- List.sort <$> listDirectory clientTmpdir
            unless
              (preDownloadClientSideFileNames == List.sort (Map.elems expectedPreDownloadFileNames))
              $ error
              $ "Pre-download contents ("
                ++ show preDownloadClientSideFileNames
                ++ ") don't match expectation ("
                ++ show (List.sort (Map.elems expectedPreDownloadFileNames))
                ++ ")"
            preDownloadContentErrors <-
              catMaybes
                <$> mapM (\fn -> hasContent ClientSide $ clientTmpdir </> fn) preDownloadClientSideFileNames
            case preDownloadContentErrors of
              [] -> do
                runDownloadChunk nullTracer setup targetChunk
                filesAfterDownload <- List.sort <$> listDirectory clientTmpdir
                if filesAfterDownload /= allFileNames
                  then
                    pure $
                      counterexample
                        ( "After download, observed filenames ("
                            ++ show filesAfterDownload
                            ++ ") don't match expected filenames ("
                            ++ show allFileNames
                        )
                        False
                  else do
                    errorsFromExpectationOfStasis <-
                      catMaybes
                        <$> mapM (\fn -> hasContent ClientSide $ clientTmpdir </> fn) preDownloadClientSideFileNames
                    errorsFromExpectationOfDownload <-
                      catMaybes
                        <$> mapM
                          (\fn -> hasContent ServerSide $ clientTmpdir </> fn)
                          (filter (`notElem` preDownloadClientSideFileNames) allFileNames)
                    pure $ case errorsFromExpectationOfStasis ++ errorsFromExpectationOfDownload of
                      [] -> property True
                      errors ->
                        counterexample ("Violation(s) of post-download file content expectations: " ++ show errors) False
              errors -> error $ "Violation(s) of pre-download file content expectations: " ++ show errors
 where
  gen_target_chunk_and_file_kernels :: Gen (ChunkNo, [FileKernel])
  gen_target_chunk_and_file_kernels = do
    targetChunk <- arbitrary
    presentClientSideFileTypes <- sublistOf currentFileTypes
    let absentClientSideFileTypes = filter (`notElem` presentClientSideFileTypes) currentFileTypes
    presentServerSideFileTypes <-
      List.nub . (++ absentClientSideFileTypes) <$> sublistOf currentFileTypes
    return
      ( targetChunk
      , [ (s, t, targetChunk)
        | (s, t) <-
            map (ServerSide,) presentServerSideFileTypes ++ map (ClientSide,) presentClientSideFileTypes
        ]
      )
  hasContent :: ConnectionSide -> FilePath -> IO (Maybe String)
  hasContent side file = do
    obsContent <- Text.unpack <$> TIO.readFile file
    pure $
      if obsContent == expContent
        then Nothing
        else
          Just $ "For file " ++ file ++ ", expected '" ++ expContent ++ "' but got '" ++ obsContent ++ "'"
   where
    expContent = getTmpfileContent side

----------------------------- Helper functions and types -----------------------------
tmpSub :: String
tmpSub = "download-test"

tracerToStdout :: Show a => Tracer IO a
tracerToStdout = showTracing stdoutTracer

tracerToFile :: Show a => FilePath -> Tracer IO a
tracerToFile f = Tracer (\a -> appendFile f (show a ++ "\n"))

fileDoesNotExist :: FilePath -> IO Bool
fileDoesNotExist f = not <$> doesFileExist f

data ConnectionSide = ServerSide | ClientSide deriving (Show, Eq, Ord)

getTmpfileContent :: ConnectionSide -> String
getTmpfileContent = ("file on " ++) . show

nameFile :: FileType -> ChunkNo -> String
nameFile ft cn = Text.unpack $ getFileName ft cn

runDownloadChunk :: Tracer IO TraceRemoteStorageEvent -> TestFolderSetup -> ChunkNo -> IO ()
runDownloadChunk tracer TestFolderSetup{..} targetChunk =
  testWithApplication (pure $ staticApp $ defaultFileServerSettings serverTmpdir) $
    \port -> do
      let storageConfig =
            RemoteStorageConfig{rscSrcUrl = "http://localhost:" ++ show port ++ "/", rscDstDir = clientTmpdir}
      downloadChunk tracer storageConfig targetChunk

setupFilesAndFolders :: FilePath -> [FileKernel] -> IO TestFolderSetup
setupFilesAndFolders tmpRoot fileKernels = do
  let getFolder side = tmpRoot </> show side
      mkfile :: FileKernel -> IO FilePath
      mkfile (s, t, c) = let fp = getFolder s </> nameFile t c in writeFile fp (getTmpfileContent s) >> pure fp
      serverTmpdir = getFolder ServerSide
      clientTmpdir = getFolder ClientSide
  forM_ [serverTmpdir, clientTmpdir] (False `createDirectoryIfMissing`)
  filepaths <- foldM (\acc k -> (: acc) <$> mkfile k) [] fileKernels
  missingFiles <- filterM fileDoesNotExist filepaths
  case missingFiles of
    [] -> pure ()
    missing -> error $ "Not all files to create have been created; missing: " ++ show missing
  return $ TestFolderSetup{serverTmpdir = serverTmpdir, clientTmpdir = clientTmpdir}

type FileKernel = (ConnectionSide, FileType, ChunkNo)

data TestFolderSetup = TestFolderSetup
  { serverTmpdir :: FilePath
  , clientTmpdir :: FilePath
  }
  deriving (Show, Eq)

instance Ord FileType where
  compare = comparing index
   where
    index t = fromMaybe (error $ "Failed to lookup index of type: " ++ show t) $ List.elemIndex t allFileTypes

----------------------------- Property aggregation -----------------------------
tests :: TestTree
tests =
  testGroup
    "Download-related utilities"
    [ testProperty
        "downloadChunk downloads requested files as expected when available"
        prop_downloadChunk_downloads_files_as_expected_when_available
    , testProperty
        "downloadChunk traces download errors correctly"
        prop_downloadChunk_traces_errors_as_expected_when_files_are_unavailable
    , testProperty
        "downloadChunk correctly handles mixed local preexistence of files"
        prop_downloadChunk_correctly_handles_mixed_local_preexistence_of_files
    ]