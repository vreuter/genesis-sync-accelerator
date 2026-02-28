module GenesisSyncAccelerator.Util (fpToHasFS, getTopLevelConfig) where

import qualified Cardano.Tools.DBAnalyser.Block.Cardano as Cardano
import Cardano.Tools.DBAnalyser.HasAnalysis (mkProtocolInfo)
import Ouroboros.Consensus.Node.ProtocolInfo (ProtocolInfo (..))
import System.FS.API (HasFS)
import System.FS.API.Types (MountPoint (MountPoint))
import System.FS.IO (HandleIO, ioHasFS)

import GenesisSyncAccelerator.Types (StandardTopLevelConfig)

fpToHasFS :: FilePath -> HasFS IO HandleIO
fpToHasFS = ioHasFS . MountPoint

getTopLevelConfig :: FilePath -> IO StandardTopLevelConfig
getTopLevelConfig configFile = pInfoConfig <$> mkProtocolInfo (Cardano.CardanoBlockArgs configFile Nothing)
