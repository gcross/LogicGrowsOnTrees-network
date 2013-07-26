-- Language extensions {{{
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE UnicodeSyntax #-}
{-# LANGUAGE ViewPatterns #-}
-- }}}

-- Imports {{{
import Control.Monad (forever,replicateM_,void)
import Control.Monad.IO.Class (MonadIO(..))

import Control.Concurrent (forkIO)
import Control.Concurrent.STM (atomically,modifyTVar,newTVarIO,readTVar,writeTVar)

import Data.Functor ((<$>))
import Data.IORef (modifyIORef,newIORef,readIORef,writeIORef)
import Data.Monoid ((<>),mempty)

import GHC.Conc (unsafeIOToSTM)

import Network (HostName,PortID(..))

import qualified System.Log.Logger as Logger
import System.Log.Logger (Priority(DEBUG,INFO),rootLoggerName,setLevel,updateGlobalLogger)
import System.Log.Logger.TH
import System.Random (randomRIO)

import Test.Framework
import Test.Framework.Providers.HUnit
import Test.HUnit hiding (Test,Path)

import LogicGrowsOnTrees
import LogicGrowsOnTrees.Checkpoint
import LogicGrowsOnTrees.Examples.Queens
import LogicGrowsOnTrees.Parallel.Adapter.Network
import LogicGrowsOnTrees.Parallel.Common.ExplorationMode
import LogicGrowsOnTrees.Parallel.Common.Purity (Purity(Pure))
import LogicGrowsOnTrees.Parallel.Common.Supervisor.RequestQueue
import LogicGrowsOnTrees.Parallel.Main
import LogicGrowsOnTrees.Utils.WordSum
-- }}}

-- Logging Functions {{{
deriveLoggers "Logger" [DEBUG,INFO]
-- }}}

-- Helper Functions {{{
remdups :: (Eq a) => [a] -> [a] -- {{{
remdups []  =  []
remdups (x : []) =  [x]
remdups (x : xx : xs)
 | x == xx   = remdups (x : xs)
 | otherwise = x : remdups (xx : xs)
-- }}}
-- }}}

main = do
    -- updateGlobalLogger rootLoggerName (setLevel DEBUG)
    defaultMain tests

tests = -- {{{
    [testCase "one process" . runTest $ \changeNumberOfWorkers → do
        changeNumberOfWorkers (\i → 0)
        changeNumberOfWorkers (\i → 1)
    ,testCase "two processes" . runTest $ \changeNumberOfWorkers → do
        changeNumberOfWorkers (\i → 3-i)
    ,testCase "many processes" . runTest $ \changeNumberOfWorkers → liftIO (randomRIO (0,1::Int)) >>= \i → case i of
        0 → changeNumberOfWorkers (\i → if i > 1 then i-1 else i)
        1 → changeNumberOfWorkers (+1)
    ]
  where
    runTest generateNoise = do
        let tree = nqueensCount 15
            port_id = PortNumber 54210
        progresses_ref ← newIORef []
        worker_ids_var ← newTVarIO []
        let changeNumberOfWorkers computeNewNumberOfWorkers = do
                old_number_of_workers ← liftIO . atomically $ length <$> readTVar worker_ids_var
                let new_number_of_workers = computeNewNumberOfWorkers old_number_of_workers
                case new_number_of_workers `compare` old_number_of_workers of
                    EQ → return ()
                    GT → liftIO
                         .
                         replicateM_ (new_number_of_workers-old_number_of_workers)
                         .
                         forkIO
                         .
                         unsafeRunNetwork
                         $
                         runWorker
                            AllMode
                            Pure
                            tree
                            "localhost"
                            port_id
                    LT → go (old_number_of_workers-new_number_of_workers)
                      where
                        go 0 = return ()
                        go n =
                            (liftIO . atomically $ do
                                worker_ids ← readTVar worker_ids_var
                                let number_of_workers = length worker_ids
                                index_to_remove ← unsafeIOToSTM $ randomRIO (0,number_of_workers-1)
                                writeTVar worker_ids_var $ take index_to_remove worker_ids ++ drop (number_of_workers+1) worker_ids
                                return $ worker_ids !! index_to_remove
                            ) >>= disconnectWorker
            notifyConnected worker_id = do
                atomically $ modifyTVar worker_ids_var (worker_id:)
                return True
            notifyDisconnected _ = return ()
        RunOutcome _ termination_reason ←
            unsafeRunNetwork $
            runSupervisor
                AllMode
                (const $ return ())
                NetworkCallbacks{..}
                port_id
                mempty
                (forever $ requestProgressUpdate >>= (liftIO . modifyIORef progresses_ref . (:)) >> generateNoise changeNumberOfWorkers)
        result ← case termination_reason of
            Aborted _ → error "prematurely aborted"
            Completed result → return result
            Failure _ message → error message
        let correct_result = exploreTree tree
        result @?= correct_result
        progresses ← remdups <$> readIORef progresses_ref
        replicateM_ 4 $ randomRIO (0,length progresses-1) >>= \i → do
            let Progress checkpoint result = progresses !! i
            result @=? exploreTreeStartingFromCheckpoint (invertCheckpoint checkpoint) tree
            correct_result @=? result <> (exploreTreeStartingFromCheckpoint checkpoint tree)
-- }}}
