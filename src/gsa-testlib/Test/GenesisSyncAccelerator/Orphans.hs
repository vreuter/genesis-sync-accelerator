-- | Typeclass instances
module Test.GenesisSyncAccelerator.Orphans () where

import Ouroboros.Consensus.Storage.ImmutableDB.Chunks.Internal
  ( ChunkNo (..)
  )
import Test.QuickCheck

-- | This picks an 'EpochNo' between 0 and 10000
--
-- We don't pick larger values because we're not interested in testing overflow
-- due to huge epoch numbers and even huger slot numbers.
instance Arbitrary ChunkNo where
  arbitrary = ChunkNo <$> choose (0, 10000)
  shrink = genericShrink
