-- Language extensions {{{
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE Rank2Types #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UnicodeSyntax #-}
-- }}}

module Control.Visitor.Parallel.BackEnd.Network -- {{{
    ( Network(..)
    , NetworkCallbacks(..)
    , NetworkControllerMonad(..)
    , NetworkMonad
    , NetworkRequestQueueMonad(..)
    , WorkerId(..)
    , runNetwork
    , runSupervisor
    , runSupervisorWithStartingProgress
    , runWorkerWithVisitor
    , runWorkerWithVisitorIO
    , runWorkerWithVisitorT
    , showPortID
    ) where -- }}}

-- Imports {{{
import Prelude hiding (catch)

import Control.Applicative (Applicative)
import Control.Concurrent (ThreadId,forkIO,killThread,myThreadId,throwTo)
import Control.Exception (AsyncException(..),SomeException,bracket,catch,fromException)
import Control.Lens (use)
import Control.Lens.Operators ((%=))
import Control.Lens.TH (makeLenses)
import Control.Monad (forever)
import Control.Monad.CatchIO (MonadCatchIO)
import Control.Monad.IO.Class (MonadIO(..))
import Control.Monad.State.Class (MonadState(..))
import Control.Monad.Trans.Reader (ask,runReaderT)
import Control.Monad.Trans.State.Strict (StateT,evalStateT)

import Data.Composition ((.*))
import Data.Functor ((<$>))
import qualified Data.Map as Map
import Data.Map (Map)
import Data.Maybe (fromMaybe)
import Data.Monoid (Monoid(..))
import Data.Serialize (Serialize)
import qualified Data.Set as Set
import Data.Set (Set)
import Data.Typeable (Typeable)

import Network (HostName,PortID(..),PortNumber,accept,connectTo,listenOn,sClose,withSocketsDo)

import System.IO (Handle)
import qualified System.Log.Logger as Logger
import System.Log.Logger (Priority(DEBUG,INFO))
import System.Log.Logger.TH

import Text.Printf

import Control.Visitor
import Control.Visitor.Checkpoint
import Control.Visitor.Parallel.Common.Message
import Control.Visitor.Parallel.Common.Process (runWorkerUsingHandles)
import qualified Control.Visitor.Parallel.Common.Supervisor as Supervisor
import Control.Visitor.Parallel.Common.Supervisor hiding (runSupervisor)
import Control.Visitor.Parallel.Common.Supervisor.RequestQueue
import Control.Visitor.Parallel.Common.Worker
import Control.Visitor.Parallel.Main
import Control.Visitor.Utils.Handle
import Control.Visitor.Workload
-- }}}

-- Logging Functions {{{
deriveLoggers "Logger" [DEBUG,INFO]
-- }}}

-- Types {{{

data Worker = Worker -- {{{
    {   workerHandle :: Handle
    ,   workerThreadId :: ThreadId
    } deriving (Eq,Show)
-- }}}

data WorkerId = WorkerId -- {{{
    {   workerHostName :: HostName
    ,   workerPortNumber :: PortNumber
    } deriving (Eq,Ord,Show,Typeable)
-- }}}

newtype Network α = Network { unwrapNetwork :: IO α }
  deriving (Applicative,Functor,Monad,MonadIO)

data NetworkCallbacks = NetworkCallbacks -- {{{
    {   notifyConnected :: WorkerId → IO Bool
    ,   notifyDisconnected :: WorkerId → IO ()
    }
-- }}}

data NetworkState = NetworkState -- {{{
    {   _pending_quit :: !(Set WorkerId)
    ,   _workers :: !(Map WorkerId Worker)
    }
makeLenses ''NetworkState
-- }}}

type NetworkStateMonad = StateT NetworkState IO

type NetworkRequestQueue result = RequestQueue result WorkerId NetworkStateMonad

type NetworkMonad result = SupervisorMonad result WorkerId NetworkStateMonad

newtype NetworkControllerMonad result α = C -- {{{
    { unwrapC :: RequestQueueReader result WorkerId NetworkStateMonad α
    } deriving (Applicative,Functor,Monad,MonadCatchIO,MonadIO)
-- }}}

-- }}}

-- Classes {{{
class RequestQueueMonad m ⇒ NetworkRequestQueueMonad m where -- {{{
    disconnectWorker :: WorkerId → m ()
-- }}}
-- }}}

-- Instances {{{
instance RequestQueueMonad (NetworkControllerMonad result) where -- {{{
    type RequestQueueMonadResult (NetworkControllerMonad result) = result
    abort = C abort
    fork = C . fork . unwrapC
    getCurrentProgressAsync = C . getCurrentProgressAsync
    getNumberOfWorkersAsync = C . getNumberOfWorkersAsync
    requestProgressUpdateAsync = C . requestProgressUpdateAsync
-- }}}
instance NetworkRequestQueueMonad (NetworkControllerMonad result) where -- {{{
    disconnectWorker worker_id = C $ ask >>= (enqueueRequest $
        debugM ("Disconnecting worker " ++ show worker_id)
        >>
{-
        use workers
        >>=
        (\workers →
            case Map.lookup worker_id workers of
                Just worker → return worker
                Nothing → error $ "Unable to find worker " ++ show worker_id ++ " to disconnect in " ++ show workers
        )
-}
        fromJustOrBust ("Unable to find worker " ++ show worker_id ++ " to disconnect.")
        .
        Map.lookup worker_id
        <$>
        use workers
        >>=
        liftIO . flip send QuitWorker . workerHandle
     )
-- }}}
-- }}}

-- Exposed Functions {{{
runNetwork :: Network α → IO α -- {{{
runNetwork = withSocketsDo . unwrapNetwork
-- }}}

runSupervisor :: -- {{{
    ( Monoid result
    , Serialize result
    ) ⇒
    NetworkCallbacks →
    PortID →
    NetworkControllerMonad result () →
    Network (RunOutcome result)
runSupervisor callbacks port_id = runSupervisorWithStartingProgress callbacks port_id mempty
-- }}}

runSupervisorWithStartingProgress :: -- {{{
    ∀ result.
    ( Monoid result
    , Serialize result
    ) ⇒
    NetworkCallbacks →
    PortID →
    Progress result →
    NetworkControllerMonad result () →
    Network (RunOutcome result)
runSupervisorWithStartingProgress NetworkCallbacks{..} port_id starting_progress (C controller) = liftIO $ do
    request_queue ← newRequestQueue
    -- Message receivers {{{
    let receiveStolenWorkloadFromWorker = flip enqueueRequest request_queue .* receiveStolenWorkload
        receiveProgressUpdateFromWorker = flip enqueueRequest request_queue .* receiveProgressUpdate
        receiveFailureFromWorker = flip enqueueRequest request_queue .* receiveWorkerFailure
        receiveFinishedFromWorker worker_id final_progress = flip enqueueRequest request_queue $ do -- {{{
            removal_flag ← Set.member worker_id <$> use pending_quit
            infoM $ if removal_flag
                then "Worker " ++ show worker_id ++ " has finished, and will be removed."
                else "Worker " ++ show worker_id ++ " has finished, and will look for another workload."
            receiveWorkerFinishedWithRemovalFlag removal_flag worker_id final_progress
        -- }}}
        receiveQuitFromWorker worker_id = flip enqueueRequest request_queue $ do -- {{{
            infoM $ "Worker " ++ show worker_id ++ " has quit."
            pending_quit %= Set.delete worker_id
            workers %= Map.delete worker_id
            removeWorkerIfPresent worker_id
            liftIO $ notifyDisconnected worker_id
        -- }}}
    -- }}}
    -- Supervisor callbacks {{{
        sendMessageToWorker message worker_id = do
            use workers
            >>=
            liftIO
            .
            flip send message
            .
            workerHandle
            .
            fromJustOrBust ("Error looking up " ++ show worker_id ++ " to send a message")
            .
            Map.lookup worker_id
        broadcastMessageToWorkers message = mapM_ (sendMessageToWorker message)
        broadcastProgressUpdateToWorkers = broadcastMessageToWorkers RequestProgressUpdate
        broadcastWorkloadStealToWorkers = broadcastMessageToWorkers RequestWorkloadSteal
        receiveCurrentProgress = receiveProgress request_queue
        sendWorkloadToWorker workload worker_id = do
            infoM $ "Activating worker " ++ show worker_id ++ " with workload " ++ show workload
            sendMessageToWorker (StartWorkload workload) worker_id
    -- }}}
    let port_id_description = showPortID port_id
    supervisor_thread_id ← myThreadId
    acceptor_thread_id ← forkIO $
        bracket
            (debugM ("Acquiring lock on " ++ port_id_description) >> listenOn port_id)
            (\socket → debugM ("Releasing lock on " ++ port_id_description) >> sClose socket)
            (\socket → forever $
                accept socket >>=
                \(workerHandle,workerHostName,workerPortNumber) → do
                    let identifier = workerHostName ++ ":" ++ show workerPortNumber
                    debugM $ "Received connection from " ++ identifier
                    let worker_id = WorkerId{..}
                        sendToWorker = send workerHandle
                    allowed_to_connect ← liftIO $ notifyConnected worker_id
                    if allowed_to_connect
                        then 
                         do debugM $ identifier ++ " is allowed to connect."
                            workerThreadId ← forkIO (
                                receiveAndProcessMessagesFromWorkerUsingHandle
                                    MessageForSupervisorReceivers{..}
                                    workerHandle
                                    worker_id
                                `catch`
                                (\e → case fromException e of
                                    Just ThreadKilled → sendToWorker QuitWorker
                                    Just UserInterrupt → sendToWorker QuitWorker
                                    _ → do enqueueRequest (removeWorker worker_id) request_queue
                                           sendToWorker QuitWorker `catch` (\(e::SomeException) → return ())
                                           liftIO $ notifyDisconnected worker_id
                                )
                             )
                            flip enqueueRequest request_queue $ do
                                workers %= Map.insert worker_id Worker{..}
                                addWorker worker_id
                        else
                         do debugM $ identifier ++ " is *not* allowed to connect."
                            sendToWorker QuitWorker
            )
        `catch`
        (\e → case fromException e of
            Just ThreadKilled → return ()
            _ → throwTo supervisor_thread_id e
        )
    manager_thread_id ← forkIO $ runReaderT controller request_queue
    flip evalStateT (NetworkState mempty mempty) $ do
        supervisor_outcome@SupervisorOutcome{supervisorRemainingWorkers} ←
            runSupervisorStartingFrom
                starting_progress
                SupervisorCallbacks{..}
                (requestQueueProgram (return ()) request_queue)
        broadcastMessageToWorkers QuitWorker supervisorRemainingWorkers
        liftIO $ killThread acceptor_thread_id
        return $ extractRunOutcomeFromSupervisorOutcome supervisor_outcome
-- }}}

runWorkerWithVisitor :: -- {{{
    (Monoid result, Serialize result) ⇒
    Visitor result →
    HostName →
    PortID →
    Network ()
runWorkerWithVisitor = genericRunWorker . flip forkVisitorWorkerThread
-- }}}
    
runWorkerWithVisitorIO :: -- {{{
    (Monoid result, Serialize result) ⇒
    VisitorIO result →
    HostName →
    PortID →
    Network ()
runWorkerWithVisitorIO = genericRunWorker . flip forkVisitorIOWorkerThread
-- }}}
    
runWorkerWithVisitorT :: -- {{{
    (Monoid result, Serialize result, MonadIO m) ⇒
    (∀ α. m α → IO α) →
    VisitorT m result →
    HostName →
    PortID →
    Network ()
runWorkerWithVisitorT runInIO = genericRunWorker . flip (forkVisitorTWorkerThread runInIO)
-- }}}

showPortID :: PortID → String -- {{{
showPortID (Service service_name) = "Service " ++ service_name
showPortID (PortNumber port_number) = "Port Number " ++ show port_number
showPortID (UnixSocket unix_socket_name) = "Unix Socket " ++ unix_socket_name
-- }}}
-- }}}

-- Internal Functions {{{
fromJustOrBust message = fromMaybe (error message)

genericRunWorker :: -- {{{
    (Monoid result, Serialize result) ⇒
    (
        (WorkerTerminationReason result → IO ()) →
        Workload →
        IO (WorkerEnvironment result)
    ) →
    HostName →
    PortID →
    Network ()
genericRunWorker spawnWorker host_name port_id = do
    debugM "called genericRunWorker"
    liftIO $ do
        handle ← connectTo host_name port_id
        runWorkerUsingHandles handle handle spawnWorker
-- }}}
-- }}}
