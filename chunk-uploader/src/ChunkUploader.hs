{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PackageImports #-}
{-# LANGUAGE PatternGuards #-}
{-# LANGUAGE ScopedTypeVariables #-}

module ChunkUploader
  ( runUploader
  ) where

import ChunkUploader.Detection (scanCompletedChunks)
import ChunkUploader.S3 (S3Handle, credentialsWork, initS3, uploadChunkTriplet)
import ChunkUploader.State
  ( defaultStateFile
  , loadState
  , saveState
  )
import ChunkUploader.Types
  ( ChunkNo
  , TraceUploaderEvent (..)
  , UploaderConfig (..)
  )
import Control.Concurrent (threadDelay)
import Control.Exception (SomeAsyncException (..), SomeException, fromException, throwIO, try)
import Control.Monad (unless)
import Data.Maybe (fromMaybe)
import System.Exit (exitFailure)
import "contra-tracer" Control.Tracer (Tracer, traceWith)

-- | Maximum number of retry attempts per chunk upload.
maxRetries :: Int
maxRetries = 3

-- | Initial retry delay, in seconds.
initialRetryDelaySecs :: Int
initialRetryDelaySecs = 1

-- | Maximum retry delay, in seconds.
maxRetryDelaySecs :: Int
maxRetryDelaySecs = 30

-- | Delay for a given retry attempt using exponential backoff.
delayForAttempt :: Int -> IO ()
delayForAttempt attempt = threadDelay $ 1_000_000 * min maxRetryDelaySecs (initialRetryDelaySecs * 2 ^ attempt)

-- | Run the main upload loop. This function does not return.
runUploader :: Tracer IO TraceUploaderEvent -> UploaderConfig -> IO ()
runUploader tracer cfg = do
  s3 <- initS3 cfg >>= either (\e -> traceWith tracer (TraceS3InitFailure e) >> exitFailure) pure
  ok <- credentialsWork s3
  unless ok $ do
    traceWith tracer (TraceCredentialValidationFailure "HeadBucket request failed")
    exitFailure
  let stateFile = fromMaybe (defaultStateFile $ ucImmutableDir cfg) (ucStateFile cfg)
  lastUploaded <- loadState stateFile
  traceWith tracer (TraceStateLoaded lastUploaded)
  loop tracer cfg s3 stateFile lastUploaded

loop ::
  Tracer IO TraceUploaderEvent ->
  UploaderConfig ->
  S3Handle ->
  FilePath ->
  Maybe ChunkNo ->
  IO ()
loop tracer cfg s3 stateFile lastUploaded = do
  traceWith tracer TraceScanStart
  completed <- scanCompletedChunks (ucImmutableDir cfg)
  traceWith tracer (TraceScanComplete completed)
  let newChunks = case lastUploaded of
        Nothing -> completed
        Just n -> filter (> n) completed
  newLast <- uploadChunks tracer s3 cfg stateFile lastUploaded newChunks
  threadDelay (ucPollInterval cfg * 1_000_000)
  loop tracer cfg s3 stateFile newLast

uploadChunks ::
  Tracer IO TraceUploaderEvent ->
  S3Handle ->
  UploaderConfig ->
  FilePath ->
  Maybe ChunkNo ->
  [ChunkNo] ->
  IO (Maybe ChunkNo)
uploadChunks _ _ _ _ current [] = pure current
uploadChunks tracer s3 cfg stateFile _ (cn : rest) = do
  success <- uploadChunkWithRetry tracer s3 cfg cn 0
  if success
    then do
      saveState stateFile cn
      traceWith tracer (TraceStateSaved cn)
      uploadChunks tracer s3 cfg stateFile (Just cn) rest
    else
      -- Stop uploading on failure; will retry next poll cycle
      pure (Just cn)

uploadChunkWithRetry ::
  Tracer IO TraceUploaderEvent ->
  S3Handle ->
  UploaderConfig ->
  ChunkNo ->
  Int ->
  IO Bool
uploadChunkWithRetry tracer s3 cfg cn attempt = do
  traceWith tracer (TraceUploadStart cn)
  result <- try $ uploadChunkTriplet s3 cn (ucImmutableDir cfg)
  case result of
    Right () -> do
      traceWith tracer (TraceUploadSuccess cn)
      pure True
    Left (e :: SomeException)
      | Just (SomeAsyncException _) <- fromException e -> throwIO e
      | otherwise -> do
          traceWith tracer (TraceUploadFailure cn (show e) (show attempt))
          if attempt < maxRetries
            then do
              delayForAttempt attempt
              traceWith tracer (TraceUploadRetry cn (attempt + 1))
              uploadChunkWithRetry tracer s3 cfg cn (attempt + 1)
            else pure False
