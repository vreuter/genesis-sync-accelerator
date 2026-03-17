module Test.GenesisSyncAccelerator.Types (ConfigFile (..), TmpDir (..), PartialOnDemandConfig (..)) where

import GenesisSyncAccelerator.Types (MaxCachedChunksCount, PrefetchChunksCount)
import Ouroboros.Consensus.Storage.ImmutableDB.Chunks.Internal (ChunkInfo)

newtype ConfigFile = ConfigFile {unConfigFile :: FilePath} deriving Show

newtype TmpDir = TmpDir {unTmpDir :: FilePath} deriving Show

data PartialOnDemandConfig = PartialOnDemandConfig
  { podcChunkInfo :: ChunkInfo
  , podcIntegrityConstant :: Bool
  , podcMaxCachedChunks :: MaxCachedChunksCount
  , podcPrefetchAhead :: PrefetchChunksCount
  }
  deriving Show
