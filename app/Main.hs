module Main where

import qualified Data.Set as Set

import GHC.Conc (numCapabilities, myThreadId)

import Lib

-- TESTING
main :: IO ()
main = do
  -- Show number of cores
  putStrLn $ "Running " ++ show (length sampleTasks) ++ " tasks on " ++ show numCapabilities ++ " CPU cores"

  -- Run
  run sampleTasks >>= print

-- Sample tasks
-- The task dependencies are something like -
--     1
--    /|\
--   2 3 4
--    /|\
--   5 6 7 8
sampleTasks :: [Task String]
sampleTasks = [
  dummyTask
    ["input1a.txt"]
    ["output1a.txt", "output1b.txt"]
    1
    "OUTPUT 1"
  ,
  dummyTask
    ["output1a.txt"]
    ["output2a.txt", "output2b.txt"]
    2
    "OUTPUT 2"
  ,
  dummyTask
    -- Use this input value to see cyclic deps error message
    -- ["output7b.txt"]
    ["output1b.txt"]
    ["output3a.txt", "output3b.txt"]
    3
    "OUTPUT 3"
  ,
  dummyTask
    ["output1a.txt"]
    ["output4a.txt", "output4b.txt"]
    4
    "OUTPUT 4"
  ,
  dummyTask
    ["output1b.txt"]
    ["output5a.txt", "output5b.txt"]
    5
    "OUTPUT 5"
  ,
  dummyTask
    ["output3b.txt"]
    ["output6a.txt", "output6b.txt", "output6c.txt"]
    6
    "OUTPUT 6"
  ,
  dummyTask
    ["output3b.txt"]
    ["output7a.txt", "output7b.txt", "output7c.txt"]
    7
    "OUTPUT 7"
  ,
  dummyTask
    ["output3b.txt"]
    ["output8a.txt", "output8b.txt", "output8c.txt"]
    8
    "OUTPUT 8"
  ]

-- Generic dummy task
-- A Task that reads from a list of files and concatenates contents
-- Prints the contents
-- and writes the contents to a list of files
-- Also performs an expensive computation to observe race conditions
-- And returns the specified value
dummyTask :: [String] -> [String] -> Int -> a -> Task a
dummyTask inputFiles outputFiles tid val = Task
  { action = do
      log "Running"
      fib tid 40 `seq` return ()
      log "Computation finished"
      concatAction
      return val
  , inputs = Set.fromList inputFiles
  , outputs = Set.fromList outputFiles
  }
  where
    log s = do
      thread <- myThreadId
      putStrLn $ "#" ++ show tid ++ "{" ++ show thread ++ "}> " ++ s
    concatAction = do
      -- Lazy read files
      log $ "Reading files " ++ show inputFiles
      contents <- mapM readFile inputFiles
      let content = concat contents
      log $ "Contents " ++ show content
      mapM_ (`writeFile` content) outputFiles
      log $ "Wrote files " ++ show outputFiles


-- An uncacheable computationally intensive task
fib :: Int -> Int -> Int
fib i 0 = i
fib i 1 = i
fib i n = let x = fib i (n-1) + fib i (n-2) in x `seq` i
