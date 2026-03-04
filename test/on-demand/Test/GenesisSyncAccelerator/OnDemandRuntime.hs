{-# LANGUAGE ExplicitForAll #-}
{-# LANGUAGE PackageImports #-}
{-# LANGUAGE RecordWildCards #-}
{-# OPTIONS_GHC -Wno-orphans #-}

module Test.GenesisSyncAccelerator.OnDemandRuntime (tests) where

import Control.Concurrent.Class.MonadSTM.Strict.TVar.Checked (readTVarIO)
import Control.Monad.Catch (MonadMask)
import Control.Monad.IO.Class (MonadIO)
import GenesisSyncAccelerator.OnDemand
  ( OnDemandConfig (..)
  , OnDemandRuntime (..)
  , OnDemandState (..)
  , newOnDemandRuntime
  )
import GenesisSyncAccelerator.RemoteStorage (RemoteStorageConfig (..))
import GenesisSyncAccelerator.Types (StandardBlock)
import GenesisSyncAccelerator.Util (fpToHasFS, getTopLevelConfig)
import Network.Wai.Application.Static (defaultFileServerSettings, staticApp)
import Network.Wai.Handler.Warp (testWithApplication)
import Ouroboros.Consensus.Config (configCodec)
import Ouroboros.Consensus.Storage.ImmutableDB.Chunks.Internal (ChunkInfo (..), ChunkSize (..))
import Paths_genesis_sync_accelerator (getDataFileName)
import System.FS.IO (HandleIO)
import System.FilePath (takeDirectory, (</>))
import qualified System.IO.Temp as Temp
import Test.QuickCheck
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.QuickCheck (testProperty)
import "contra-tracer" Control.Tracer (nullTracer)

----------------------------- Generators and properties -----------------------------
instance Arbitrary ChunkInfo where
  arbitrary = do UniformChunkSize <$> arbitrary

instance Arbitrary ChunkSize where
  arbitrary = do
    n <- choose (1, 100)
    p <- arbitrary
    return $ ChunkSize{numRegularBlocks = n, chunkCanContainEBB = p}

prop_newOnDemandRuntimeContainsConfigInfoAsGiven :: PartialOnDemandConfig -> Property
prop_newOnDemandRuntimeContainsConfigInfoAsGiven partialConfig@PartialOnDemandConfig{..} =
  ioProperty $
    checkFromConfig partialConfig $ \original -> do
      recovered <- odrConfig <$> newOnDemandRuntime original
      return $
        conjoin
          [ odcChunkInfo recovered === podcChunkInfo
          , odcMaxCachedChunks recovered === podcMaxCachedChunks
          , odcRemote recovered === odcRemote original
          ]

prop_newOnDemandRuntimeStartsWithEmptyCachedChunks :: PartialOnDemandConfig -> Property
prop_newOnDemandRuntimeStartsWithEmptyCachedChunks partialConfig =
  ioProperty $
    checkFromConfig partialConfig $ \config -> do
      runtime <- newOnDemandRuntime config
      chunks <- odsCachedChunks <$> readTVarIO (odrState runtime)
      return $ property $ null chunks

prop_newOnDemandRuntimeStartsWithEmptyUsageOrder :: PartialOnDemandConfig -> Property
prop_newOnDemandRuntimeStartsWithEmptyUsageOrder partialConfig =
  ioProperty $
    checkFromConfig partialConfig $ \config -> do
      runtime <- newOnDemandRuntime config
      usageOrder <- odsUsageOrder <$> readTVarIO (odrState runtime)
      return $ property $ null usageOrder

----------------------------- Helper functions, types, and instances -----------------------------
instance Eq ChunkInfo where
  UniformChunkSize cs1 == UniformChunkSize cs2 = cs1 == cs2

instance Eq ChunkSize where
  (==) cs1 cs2 =
    numRegularBlocks cs1 == numRegularBlocks cs2
      && chunkCanContainEBB cs1 == chunkCanContainEBB cs2

data PartialOnDemandConfig = PartialOnDemandConfig
  { podcChunkInfo :: ChunkInfo
  , podcIntegrityConstant :: Bool
  , podcMaxCachedChunks :: Int
  }
  deriving Show

newtype ConfigFile = ConfigFile {unConfigFile :: FilePath} deriving Show

newtype TmpDir = TmpDir {unTmpDir :: FilePath} deriving Show

checkFromConfig ::
  PartialOnDemandConfig -> (OnDemandConfig IO StandardBlock HandleIO -> IO Property) -> IO Property
checkFromConfig partialConfig mkProp =
  withTemp $ \tmp -> do
    configFile <- getDataFileName ("test" </> "data" </> "config" </> "config.json")
    let dataDir = takeDirectory configFile
    testWithApplication (pure $ staticApp $ defaultFileServerSettings dataDir) $
      \port -> do
        config <- mkFullConfig partialConfig (ConfigFile configFile) (TmpDir tmp) port
        mkProp config

mkFullConfig ::
  PartialOnDemandConfig ->
  ConfigFile ->
  TmpDir ->
  Int ->
  IO (OnDemandConfig IO StandardBlock HandleIO)
mkFullConfig PartialOnDemandConfig{..} (ConfigFile configFile) (TmpDir tmpdir) port = do
  codecConfig <- configCodec <$> getTopLevelConfig configFile
  return $
    OnDemandConfig
      { odcRemote = mkRemoteStorageConfig tmpdir port
      , odcTracer = nullTracer
      , odcChunkInfo = podcChunkInfo
      , odcHasFS = fpToHasFS tmpdir
      , odcCodecConfig = codecConfig
      , odcCheckIntegrity = const podcIntegrityConstant
      , odcMaxCachedChunks = podcMaxCachedChunks
      }

instance Arbitrary PartialOnDemandConfig where
  arbitrary = do
    chunkInfo <- arbitrary
    integrity <- arbitrary
    maxChunks <- choose (1, 100)
    return
      PartialOnDemandConfig
        { podcChunkInfo = chunkInfo
        , podcIntegrityConstant = integrity
        , podcMaxCachedChunks = maxChunks
        }

mkRemoteStorageConfig :: FilePath -> Int -> RemoteStorageConfig
mkRemoteStorageConfig tmpdir port =
  RemoteStorageConfig
    { rscSrcUrl = "http://localhost:" ++ show port
    , rscDstDir = tmpdir
    }

withTemp :: forall m a. (MonadIO m, MonadMask m) => (FilePath -> m a) -> m a
withTemp = Temp.withSystemTempDirectory "on-demand-test"

----------------------------- Property aggregation -----------------------------
tests :: TestTree
tests =
  testGroup
    "On-Demand Runtime"
    [ testProperty
        "newOnDemandRuntime contains the given config info"
        prop_newOnDemandRuntimeContainsConfigInfoAsGiven
    , testProperty
        "newOnDemandRuntime starts with empty cached chunks"
        prop_newOnDemandRuntimeStartsWithEmptyCachedChunks
    , testProperty
        "newOnDemandRuntime starts with empty usageorder"
        prop_newOnDemandRuntimeStartsWithEmptyUsageOrder
    ]
