module GenesisSyncAccelerator.Types (HostAddr, StandardTopLevelConfig) where

import Data.Word (Word8)
import Ouroboros.Consensus.Cardano.Block (CardanoBlock)
import Ouroboros.Consensus.Config (TopLevelConfig)
import Ouroboros.Consensus.Protocol.TPraos (StandardCrypto)

type HostAddr = (Word8, Word8, Word8, Word8)

type StandardTopLevelConfig = TopLevelConfig (CardanoBlock StandardCrypto)