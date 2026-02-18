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

allFileTypes :: [FileType]
allFileTypes = EpochFile : currentFileTypes

currentFileTypes :: [FileType]
currentFileTypes = [ChunkFile, PrimaryIndexFile, SecondaryIndexFile]

genSeveralChunkNumbers :: Gen [ChunkNo]
genSeveralChunkNumbers = do
  n <- choose (2, 5)
  ints <- shuffle [1 .. 10]
  return $ map ChunkNo $ take n ints

getAllFilenamesForChunk :: ChunkNo -> [String]
getAllFilenamesForChunk cn = getFilenamesForChunk cn allFileTypes

getCurrentFilenamesForChunk :: ChunkNo -> [String]
getCurrentFilenamesForChunk cn = getFilenamesForChunk cn currentFileTypes

getFilenamesForChunk :: ChunkNo -> [FileType] -> [String]
getFilenamesForChunk cn = map (\ft -> Text.unpack $ getFileName ft cn)
