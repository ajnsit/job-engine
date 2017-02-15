{-# LANGUAGE MultiWayIf #-}
module Lib where

import Control.Monad (void, when, unless, forever)
import Data.List (delete, partition)
import Data.Set (Set, (\\))
import qualified Data.Set as Set

import Control.Concurrent (forkIO)
import Control.Concurrent.STM (TVar, newTVar, readTVar, writeTVar, atomically, retry)
import Control.Exception (SomeException, mask, try)

import GHC.Conc (numCapabilities)

-- A Task Pool to manage task executions
data Pool a = Pool
  -- Finished task results
  { finished :: [TaskResult a]
  -- Currently running tasks
  , running :: [RunningTask a]
  -- Tasks scheduled for execution (all inputs are available)
  -- But there are no available threads
  , sequenced :: [Node a]
  -- Tasks waiting for inputs not yet available
  , waiting :: [Node a]
  -- As tasks finish, they signal the availability of the files they outputted
  , availableInputs :: Set String
  -- Maximum number of threads to run
  , maxThreads :: Int
  -- Is the pool shutting down
  , shutdown :: Bool
  }

-- We identify all tasks with an Int tag
type TaskId = Int
-- A Task result can either be an exception or the result value
type TaskResult a = (Either SomeException a, TaskId)
-- We keep track of task ids that are running
type RunningTask a = TaskId
-- A task when waiting is assigned a taskid and becomes a node
type Node a = (Task a, TaskId)

-- A Task is part of a DAG with inputs and outputs
data Task a = Task
  { action  :: IO a
  , inputs  :: Set String
  , outputs :: Set String
  }

-- What to do next
data WhatToDo a
  = Continue
  | Shutdown [TaskResult a]
  | ErrorOut ([Node a], [TaskResult a])

-- Run a bunch of tasks
-- 1. Takes interdependencies into account, so a task that depends on
--    file input X is not executed before another task that produces file output X
-- 2. Parallelises computations whenever possible
-- Returns task results in LIFO order of tasks finishing
run :: [Task a] -> IO [TaskResult a]
run tasks = do
  poolVar <- atomically (newTVar newPool)
  -- Start threads
  let threadsToStart = maxThreads newPool
  mapM_ (const $ runNode poolVar) [1..threadsToStart]
  -- Start processing
  go poolVar
  where
    go poolVar = do
      -- Run max threads
      whatToDo <- atomically $ do
        pool <- readTVar poolVar

        -- Runnable are all tasks which depend only on currently resolved inputs
        let (runnables,rest) = partition
              (\(task,_) -> Set.null (inputs task \\ availableInputs pool))
              (waiting pool)

        -- Sequence all runnables
        unless (null runnables) $
          writeTVar poolVar pool {sequenced = sequenced pool ++ runnables, waiting = rest}
        pool <- readTVar poolVar

        if
          -- We found new runnable tasks, so run them
          | not (null runnables) -> return Continue
          -- Some tasks are still running or runnable, then just wait
          | not (null (sequenced pool) && null (running pool)) -> retry
          -- There are no running or runnable tasks, but still some tasks leftover
          -- This usually indicates cyclic deps
          | not (null (waiting pool)) -> do
            -- Shutdown the pool
            writeTVar poolVar $ pool { shutdown = True }
            return $ ErrorOut (waiting pool, finished pool)
          -- Finish when there are no more tasks left
          | otherwise -> do
            writeTVar poolVar $ pool { shutdown = True }
            return $ Shutdown (finished pool)

      case whatToDo of
        Continue -> go poolVar
        Shutdown finishedTasks -> do
          putStrLn "DONE"
          return finishedTasks
        ErrorOut (cyclicTasks, finishedTasks) -> do
          putStrLn $ "ERROR: Cyclic dependencies for tasks - " ++ show (map snd cyclicTasks)
          putStrLn "Shutting Down"
          return finishedTasks

    -- Create a new pool from the input tasks
    newPool = Pool
      { finished = []
      , running = []
      , sequenced = []
      -- Assign sequential task ids and schedule them
      , waiting = zip tasks [0..]
      -- External deps are files that are not produced by any internal process
      -- We assume that these are already available
      -- If this assumption is false, replace with availableInputs = Set.empty
      , availableInputs = concatMapSet inputs tasks \\ concatMapSet outputs tasks
      -- By default number of threads is limited to the number of cpu cores
      , maxThreads = numCapabilities
      , shutdown = False
      }

-- Run a task node
runNode :: TVar (Pool a) -> IO ()
runNode poolVar = void $ forkIO $ forever go
  where
    go = do
      mnode <- atomically $ do
        pool <- readTVar poolVar
        -- Gracefully exit thread when shutting down pool
        if | shutdown pool -> return Nothing
           -- Wait for tasks to arrive
           | null (sequenced pool) -> retry
           | otherwise -> do
              let node@(_,tid) = head (sequenced pool)
              writeTVar poolVar $
                pool
                  { running = tid:running pool
                  , sequenced = tail (sequenced pool)
                  }
              return $ Just node
      case mnode of
        Nothing -> return ()
        -- Replicate forkFinally, without actually forking
        Just (task,tid) -> mask $ \restore -> do
          -- Run the task
          result <- try (restore $ action task)
          -- Update the pool when a task exits
          -- Ensure that the task still exists in the running pool
          atomically $ do
            pool <- readTVar poolVar
            when (tid `elem` running pool) $
              writeTVar poolVar $
                pool
                  { running = delete tid (running pool)
                  , finished = (result, tid):finished pool
                  , availableInputs = Set.union (outputs task) (availableInputs pool)
                  }

-- Misc
concatMapSet :: Ord b => (a -> Set b) -> [a] -> Set b
concatMapSet f = Set.unions . map f
