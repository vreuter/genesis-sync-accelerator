{-# LANGUAGE ExplicitForAll #-}
{-# LANGUAGE PackageImports #-}
{-# LANGUAGE RecordWildCards #-}
{-# OPTIONS_GHC -Wno-orphans #-}

module Test.GenesisSyncAccelerator.OnDemandRuntime (tests) where

import Control.Concurrent.Class.MonadSTM.Strict.TVar.Checked (readTVar, readTVarIO)
import Control.Monad.Class.MonadSTM.Internal (atomically)
import GenesisSyncAccelerator.OnDemand
  ( OnDemandConfig (..)
  , OnDemandRuntime (..)
  , OnDemandState (..)
  , newOnDemandRuntime
  )
import GenesisSyncAccelerator.RemoteStorage (RemoteStorageConfig (..))
import GenesisSyncAccelerator.Tracing (RemoteStorageTracer)
import GenesisSyncAccelerator.Util (fpToHasFS, getTopLevelConfig)
import Ouroboros.Consensus.Config (configCodec)
import Ouroboros.Consensus.Storage.ImmutableDB.Chunks.Internal (ChunkInfo (..), ChunkSize (..))
import Paths_genesis_sync_accelerator (getDataFileName)
import Test.QuickCheck
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.QuickCheck (testProperty)
import "contra-tracer" Control.Tracer (nullTracer)

-- instance Arbitrary RemoteStorageConfig where
--   arbitrary = do
--     srcUrl <- arbitary
--     dstDir <- arbitrary
--     return $ RemoteStorageConfig { rscSrcUrl = srcUrl, rscDstDir = dstDir }

instance Arbitrary ChunkInfo where
  arbitrary = UniformChunkSize . ChunkSize <$> arbitrary

instance Arbitrary ChunkSize where
  arbitrary = do
    n <- choose (1, 100)
    p <- arbitrary
    return $ ChunkSize { numRegularBlocks = n, chunkCanContainEBB = p }

data PartialOnDemandConfig m = PartialOnDemandConfig
  { podcRemote :: RemoteStorageConfig
  , podcTracer :: RemoteStorageTracer m
  , podcChunkInfo :: ChunkInfo
  , podCacheDir :: FilePath
  , podcIntegrityConstant :: Bool
  , podcMaxCachedChunks :: Int
  }

mkFullConfig :: forall m blk h. PartialOnDemandConfig m -> FilePath -> IO (OnDemandConfig m blk h)
mkFullConfig PartialOnDemandConfig{..} configFile = do
  codecConfig <- configCodec <$> getTopLevelConfig configFile
  return $ OnDemandConfig
    { odcRemote = podcRemote
    , odcTracer = podcTracer
    , odcChunkInfo = podcChunkInfo
    , odcHasFS = fpToHasFS podCacheDir
    , odcCodecConfig = codecConfig
    , odcCheckIntegrity = const podcIntegrityConstant
    , odcMaxCachedChunks = podcMaxCachedChunks
    }

genPartialConfig :: forall m. Gen (PartialOnDemandConfig m)
genPartialConfig = do
  remoteConfig <- arbitrary
  chunkInfo <- arbitrary
  cacheDir <- arbitrary
  integrity <- arbitrary
  maxChunks <- choose (1, 100)
  return PartialOnDemandConfig
    { podcRemote = remoteConfig
    , podcTracer = nullTracer
    , podcChunkInfo = chunkInfo
    , podCacheDir = cacheDir
    , podcIntegrityConstant = integrity
    , podcMaxCachedChunks = maxChunks
    }

prop_newOnDemandRuntimeContainsConfigAsGiven :: PartialOnDemandConfig IO -> Property
prop_newOnDemandRuntimeContainsConfigAsGiven partialConfig = 
  ioProperty $ do
    let configFile = getDataFileName "test/data/config.json"
    config <- mkFullConfig partialConfig configFile
    odrConfig (newOnDemandRuntime config) === config

-- prop_newOnDemandRuntimeStartsWithEmptyCachedChunks :: RemoteStorageConfig -> Property
-- prop_newOnDemandRuntimeStartsWithEmptyCachedChunks config = 
--   ioProperty $ null . odsCachedChunks <$> readState $ newOnDemandRuntime config

-- prop_newOnDemandRuntimeStartsWithEmptyUsageOrder :: RemoteStorageConfig -> Property
-- prop_newOnDemandRuntimeStartsWithEmptyUsageOrder config = null $ odrUsageOrder $ newOnDemandRuntime config

readState :: forall m blk h. OnDemandRuntime m blk h -> IO (OnDemandState blk)
readState = readTVarIO . odrState

tests :: TestTree
tests =
  testGroup
    "On-Demand Runtime"
    [ testProperty
        "newOnDemandRuntime contains the given config"
        prop_newOnDemandRuntimeContainsConfigAsGiven
    -- , testProperty
    --     "newOnDemandRuntime starts with empty cached chunks"
    --     prop_newOnDemandRuntimeStartsWithEmptyCachedChunks
    -- , testProperty
    --     "newOnDemandRuntime starts with empty usageorder"
    --     prop_newOnDemandRuntimeStartsWithEmptyUsageOrder
    ]
