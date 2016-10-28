{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Demo of a data pipeline in haskell
module Main where

import Protolude
import Data.Map (Map)
import qualified System.IO as IO
import qualified Data.List as Lst
import Control.Concurrent.MVar

-- | A raised event
data ValueEvent = ValueEvent { evtValue :: Double } deriving (Show, Eq)

-- | State of the pipeline
data PipelineState = PipelineState { stateValueText :: Text
                                   , stateHistory :: [ValueEvent]
                                   }
                     deriving (Show, Eq)

-- | Type of a step
type Step = PipelineState -> ValueEvent -> PipelineState

-- | Step that does nothing
nopStep :: Step
nopStep state evt = state

-- | Step that stores history up to maxDepth. Notice that this needs to be curried to be a valid Step
addHistoryStep :: Int -> Step
addHistoryStep maxDepth state evt =
  state {stateHistory = evt : take (maxDepth - 1) (stateHistory state)}

-- | Step that calculates an average of the historic values
avgOverHistoryStep :: Step
avgOverHistoryStep state evt =
  state {stateValueText = show . avg $ evtValue <$> stateHistory state}
  where
    avg l = sum l / fromIntegral (length l)
  
-- | Create and run a pipelin
createPipeline :: Text -> [Step] -> MVar PipelineState -> IO (MVar ValueEvent)
createPipeline name steps spy = do
  mv <- newEmptyMVar 
  forkIO $ runPipeline mv PipelineState{ stateValueText = "", stateHistory = [] }
  pure mv

  where
    runPipeline :: MVar ValueEvent -> PipelineState -> IO ()
    runPipeline mv state = do
      -- Get next event
      evt <- takeMVar mv
      -- Fold event through pipeline
      let next = foldl (\st fn -> fn st evt) state steps 
      -- Spy on result
      putMVar spy next
      -- Wait for next value
      runPipeline mv next

main :: IO ()
main = do
  -- Spy MV: Just dump the state at the end of each pipeline run
  spyMv <- newEmptyMVar
  forkIO $ forever $ takeMVar spyMv >>= putText . show
  
  let pipeline1 = [nopStep, addHistoryStep 2, avgOverHistoryStep]
  mv1 <- createPipeline "pipeline1" pipeline1 spyMv
  
  forever $ do
    t <- IO.getLine
    case (readMaybe t ::Maybe Double) of
      Nothing -> putText "invalid value"
      Just d -> putMVar mv1 ValueEvent{ evtValue = d }
