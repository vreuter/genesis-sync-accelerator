module ChunkUploader.State
  ( loadState
  , saveState
  , defaultStateFile
  ) where

import ChunkUploader.Types (ChunkNo (..))
import Control.Exception (uninterruptibleMask_)
import qualified Data.Text as T
import qualified Data.Text.IO as T
import System.Directory (doesFileExist, renameFile)
import System.FilePath ((</>))
import Text.Read (readMaybe)

-- | Load the last-uploaded chunk number from the state file.
-- Returns 'Nothing' if the file doesn't exist or can't be parsed.
loadState :: FilePath -> IO (Maybe ChunkNo)
loadState fp = do
  exists <- doesFileExist fp
  if exists
    then do
      contents <- T.readFile fp
      let trimmed = T.unpack (T.strip contents)
      pure $ ChunkNo <$> readMaybe trimmed
    else pure Nothing

-- | Atomically save the last-uploaded chunk number to the state file.
-- Writes to a temporary file first, then renames for atomicity.
saveState :: FilePath -> ChunkNo -> IO ()
saveState fp (ChunkNo n) = uninterruptibleMask_ $ do
  let tmp = fp ++ ".tmp"
  writeFile tmp (show n ++ "\n")
  renameFile tmp fp

-- | Derive a default state file path from the immutable directory.
defaultStateFile :: FilePath -> FilePath
defaultStateFile immDir = immDir </> ".chunk-uploader-state"
