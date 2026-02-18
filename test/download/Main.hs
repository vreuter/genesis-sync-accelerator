module Main (main) where

import qualified Test.GenesisSyncAccelerator.Download
import Test.Tasty (TestTree, defaultMain, testGroup)

main :: IO ()
main = defaultMain tests

tests :: TestTree
tests =
  testGroup
    "download"
    [ Test.GenesisSyncAccelerator.Download.tests
    ]
