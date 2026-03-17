module Main (main) where

import qualified Test.GenesisSyncAccelerator.OnDemand.EnsureChunks
import qualified Test.GenesisSyncAccelerator.OnDemand.Runtime
import Test.Tasty (TestTree, defaultMain, testGroup)

main :: IO ()
main = defaultMain tests

tests :: TestTree
tests =
  testGroup
    "on-demand"
    [ Test.GenesisSyncAccelerator.OnDemand.EnsureChunks.tests
    , Test.GenesisSyncAccelerator.OnDemand.Runtime.tests
    ]
