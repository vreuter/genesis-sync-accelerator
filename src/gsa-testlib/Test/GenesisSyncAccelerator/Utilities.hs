-- | Utilities for test suites
module Test.GenesisSyncAccelerator.Utilities
  ( allFileTypes
  , currentFileTypes
  , genSeveralChunkNumbers
  , getAllFilenamesForChunk
  , getCurrentFilenamesForChunk
  ) where

import qualified Data.Text as Text
import GenesisSyncAccelerator.RemoteStorage (FileType (..), getFileName)
import Ouroboros.Consensus.Storage.ImmutableDB.Chunks.Internal (ChunkNo (..))
import Test.QuickCheck

-- | An exhaustive list of all file types possibly associated with a chunk.
allFileTypes :: [FileType]
allFileTypes = EpochFile : currentFileTypes

-- | A list of file types that the accelerator is currently expected to download for a chunk.
currentFileTypes :: [FileType]
currentFileTypes = [ChunkFile, PrimaryIndexFile, SecondaryIndexFile]

-- | Generate 2-5 chunk numbers, in the range 1-10, without duplicates.
genSeveralChunkNumbers :: Gen [ChunkNo]
genSeveralChunkNumbers = do
  n <- choose (2, 5)
  ints <- shuffle [1 .. 10]
  return $ map ChunkNo $ take n ints

-- | Get the name of each file possibly associated with a chunk.
getAllFilenamesForChunk :: ChunkNo -> [String]
getAllFilenamesForChunk cn = getFilenamesForChunk cn allFileTypes

-- | List the name of each file currently expected to be associated with a chunk.
getCurrentFilenamesForChunk :: ChunkNo -> [String]
getCurrentFilenamesForChunk cn = getFilenamesForChunk cn currentFileTypes

-- | For the given chunk, for the given file types, the the expected file name.
getFilenamesForChunk :: ChunkNo -> [FileType] -> [String]
getFilenamesForChunk cn = map (\ft -> Text.unpack $ getFileName ft cn)
