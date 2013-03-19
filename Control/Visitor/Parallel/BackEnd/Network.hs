-- Language extensions {{{
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
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
    , WrappedPortID(..)
    , WorkerId(..)
    , driver
    , driverNetwork
    , getConfiguration
    , runNetwork
    , runSupervisor
    , runSupervisorWithStartingProgress
    , runVisitor
    , runVisitorIO
    , runVisitorT
    , runWorkerWithVisitor
    , runWorkerWithVisitorIO
    , runWorkerWithVisitorT
    , runWorkerUsingHandleWithVisitor
    , runWorkerUsingHandleWithVisitorIO
    , runWorkerUsingHandleWithVisitorT
    , showPortID
    ) where -- }}}

-- Imports {{{
import Prelude hiding (catch)

import Control.Applicative (Applicative(..),liftA2)
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

import Data.Composition ((.*),(.********))
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

import System.Console.CmdTheLine
import System.Environment (getArgs)
import System.IO (Handle)
import qualified System.Log.Logger as Logger
import System.Log.Logger (Priority(DEBUG,INFO))
import System.Log.Logger.TH

import Text.PrettyPrint (text)

import Control.Visitor hiding (runVisitor,runVisitorT)
import Control.Visitor.Checkpoint
import Control.Visitor.Parallel.Common.Message
import qualified Control.Visitor.Parallel.Common.Process as Process
import qualified Control.Visitor.Parallel.Common.Supervisor as Supervisor
import Control.Visitor.Parallel.Common.Supervisor hiding (runSupervisor)
import Control.Visitor.Parallel.Common.Supervisor.RequestQueue
import Control.Visitor.Parallel.Common.Worker hiding (runVisitor,runVisitorIO,runVisitorT)
import Control.Visitor.Parallel.Main
import Control.Visitor.Utils.Handle
import Control.Visitor.Workload
-- }}}

-- Logging Functions {{{
deriveLoggers "Logger" [DEBUG,INFO]
-- }}}

-- Types {{{

-- Configuration {{{
newtype WrappedPortID = WrappedPortID { unwrapPortID :: PortID }

data NetworkConfiguration shared_configuration supervisor_configuration = -- {{{
    SupervisorConfiguration
    {   shared_configuration :: shared_configuration
    ,   supervisor_configuration :: supervisor_configuration
    ,   supervisor_port :: WrappedPortID
    }
  | WorkerConfiguration
    {   supervisor_host_name :: HostName
    ,   supervisor_port :: WrappedPortID
    }
-- }}}
-- }}}

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

newtype Network α = Network { unsafeRunNetwork :: IO α }
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
instance ArgVal WrappedPortID where -- {{{
    converter = (parsePortID,prettyPortID)
      where
        (parseInt,prettyInt) = converter
        parsePortID =
            either Left (\n →
                if n >= 0 && n <= (65535 :: Int)
                    then Right . WrappedPortID . PortNumber . fromIntegral $ n
                    else Left . text $ "bad port number: must be between 0 and 65535, inclusive (was given " ++ show n ++ ")"
            )
            .
            parseInt
        prettyPortID (WrappedPortID (PortNumber port_number)) = prettyInt . fromIntegral $ port_number
        prettyPortID _ = error "a non-numeric port ID somehow made its way in to the configuration"
instance ArgVal (Maybe WrappedPortID) where
    converter = just
-- }}}
-- }}}

-- Values {{{
-- Terms {{{
supervisorConfigurationTermFor shared_configuration_term supervisor_configuration_term = -- {{{
    SupervisorConfiguration
        <$> shared_configuration_term
        <*> supervisor_configuration_term
        <*> (required
             $
             opt Nothing
                ((optInfo ["p","port"])
                  { optName = "PORT"
                  , optDoc = "port on which to listen for workers"
                  }
                )
            )
-- }}}
worker_configuration_term = -- {{{
    WorkerConfiguration
        <$> (required
             $
             pos 0
                Nothing
                posInfo
                  { posName = "HOST_NAME"
                  , posDoc = "supervisor host name"
                  }
            )
        <*> (required
             $
             pos 1
                Nothing
                posInfo
                  { posName = "HOST_PORT"
                  , posDoc = "supervisor host port"
                  }
            )
-- }}}
no_configuration_term = ret (pure $ helpFail Plain Nothing)
-- }}}

default_network_callbacks = NetworkCallbacks -- {{{
    {   notifyConnected = const (return True)
    ,   notifyDisconnected = const (return ())
    }
-- }}}
-- }}}

-- Drivers {{{
driver :: ∀ shared_configuration supervisor_configuration visitor result. -- {{{
    Serialize shared_configuration ⇒
    Driver
        IO
        shared_configuration
        supervisor_configuration
        visitor
        result
driver = case (driverNetwork :: Driver Network shared_configuration supervisor_configuration visitor result) of { Driver runDriver → Driver (runNetwork .******** runDriver) }
-- }}}

driverNetwork :: Serialize shared_configuration ⇒ Driver Network shared_configuration supervisor_configuration visitor result -- {{{
driverNetwork = Driver $
    \forkVisitorWorkerThread
     shared_configuration_term
     supervisor_configuration_term
     term_info
     initializeGlobalState
     constructVisitor
     getStartingProgress
     notifyTerminated
     constructManager →
    genericRunVisitor
         forkVisitorWorkerThread
        (getConfiguration shared_configuration_term supervisor_configuration_term term_info)
         initializeGlobalState
         constructVisitor
         getStartingProgress
         constructManager
    >>=
    maybe (return ()) (liftIO . (notifyTerminated <$> fst . fst <*> snd . fst <*> snd))
-- }}}
-- }}}

-- Exposed Functions {{{
getConfiguration :: -- {{{
    Term shared_configuration →
    Term supervisor_configuration →
    TermInfo →
    IO (NetworkConfiguration shared_configuration supervisor_configuration)
getConfiguration shared_configuration_term supervisor_configuration_term term_info =
    execChoice
        (no_configuration_term,term_info)
       [(supervisorConfigurationTermFor shared_configuration_term supervisor_configuration_term,defTI
          { termName = "supervisor"
          , termDoc  = "Run the program in supervisor mode, waiting for network connections from workers on the specified port."
          }
        )
       ,(worker_configuration_term,defTI
          { termName = "worker"
          , termDoc  = "Run the program in worker mode, connecting to the specified supervisor to receive workloads."
          }
        )
       ]
-- }}}

runNetwork :: Network α → IO α -- {{{
runNetwork = withSocketsDo . unsafeRunNetwork
-- }}}

runSupervisor :: -- {{{
    ( Monoid result
    , Serialize result
    ) ⇒
    (Handle → IO ()) →
    NetworkCallbacks →
    PortID →
    NetworkControllerMonad result () →
    Network (RunOutcome result)
runSupervisor initializeWorker callbacks port_id =
    runSupervisorWithStartingProgress initializeWorker callbacks port_id mempty
-- }}}

runSupervisorWithStartingProgress :: -- {{{
    ∀ result.
    ( Monoid result
    , Serialize result
    ) ⇒
    (Handle → IO ()) →
    NetworkCallbacks →
    PortID →
    Progress result →
    NetworkControllerMonad result () →
    Network (RunOutcome result)
runSupervisorWithStartingProgress initializeWorker NetworkCallbacks{..} port_id starting_progress (C controller) = liftIO $ do
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
                            initializeWorker workerHandle
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

runVisitor :: -- {{{
    (Serialize shared_configuration, Monoid result, Serialize result) ⇒
    IO (NetworkConfiguration shared_configuration supervisor_configuration) →
    (shared_configuration → IO ()) →
    (shared_configuration → Visitor result) →
    (shared_configuration → supervisor_configuration → IO (Progress result)) →
    (shared_configuration → supervisor_configuration → NetworkControllerMonad result ()) →
    Network (Maybe ((shared_configuration,supervisor_configuration),RunOutcome result))
runVisitor getConfiguration initializeGlobalState constructVisitor getStartingProgress constructManager =
    genericRunVisitor
        forkVisitorWorkerThread
        getConfiguration
        initializeGlobalState
        constructVisitor
        getStartingProgress
        constructManager
-- }}}

runVisitorIO :: -- {{{
    (Serialize shared_configuration, Monoid result, Serialize result) ⇒
    IO (NetworkConfiguration shared_configuration supervisor_configuration) →
    (shared_configuration → IO ()) →
    (shared_configuration → VisitorIO result) →
    (shared_configuration → supervisor_configuration → IO (Progress result)) →
    (shared_configuration → supervisor_configuration → NetworkControllerMonad result ()) →
    Network (Maybe ((shared_configuration,supervisor_configuration),RunOutcome result))
runVisitorIO getConfiguration initializeGlobalState constructVisitor getStartingProgress constructManager =
    genericRunVisitor
        forkVisitorIOWorkerThread
        getConfiguration
        initializeGlobalState
        constructVisitor
        getStartingProgress
        constructManager
-- }}}

runVisitorT :: -- {{{
    (Serialize shared_configuration, Monoid result, Serialize result, Functor m, MonadIO m) ⇒
    (∀ α. m α → IO α) →
    IO (NetworkConfiguration shared_configuration supervisor_configuration) →
    (shared_configuration → IO ()) →
    (shared_configuration → VisitorT m result) →
    (shared_configuration → supervisor_configuration → IO (Progress result)) →
    (shared_configuration → supervisor_configuration → NetworkControllerMonad result ()) →
    Network (Maybe ((shared_configuration,supervisor_configuration),RunOutcome result))
runVisitorT runInBase getConfiguration initializeGlobalState constructVisitor getStartingProgress constructManager =
    genericRunVisitor
        (forkVisitorTWorkerThread runInBase)
        getConfiguration
        initializeGlobalState
        constructVisitor
        getStartingProgress
        constructManager
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

runWorkerUsingHandleWithVisitor :: -- {{{
    (Monoid result, Serialize result) ⇒
    Visitor result →
    Handle →
    Network ()
runWorkerUsingHandleWithVisitor = genericRunWorkerUsingHandle . flip forkVisitorWorkerThread
-- }}}

runWorkerUsingHandleWithVisitorIO :: -- {{{
    (Monoid result, Serialize result) ⇒
    VisitorIO result →
    Handle →
    Network ()
runWorkerUsingHandleWithVisitorIO = genericRunWorkerUsingHandle . flip forkVisitorIOWorkerThread
-- }}}

runWorkerUsingHandleWithVisitorT :: -- {{{
    (Monoid result, Serialize result, MonadIO m) ⇒
    (∀ α. m α → IO α) →
    VisitorT m result →
    Handle →
    Network ()
runWorkerUsingHandleWithVisitorT runInIO = genericRunWorkerUsingHandle . flip (forkVisitorTWorkerThread runInIO)
-- }}}

showPortID :: PortID → String -- {{{
showPortID (Service service_name) = "Service " ++ service_name
showPortID (PortNumber port_number) = "Port Number " ++ show port_number
showPortID (UnixSocket unix_socket_name) = "Unix Socket " ++ unix_socket_name
-- }}}
-- }}}

-- Internal Functions {{{
fromJustOrBust message = fromMaybe (error message)

genericRunVisitor :: -- {{{
    (Serialize shared_configuration, Monoid result, Serialize result) ⇒
    (
        (WorkerTerminationReason result → IO ()) →
        visitor result →
        Workload →
        IO (WorkerEnvironment result)
    ) →
    IO (NetworkConfiguration shared_configuration supervisor_configuration) →
    (shared_configuration → IO ()) →
    (shared_configuration → visitor result) →
    (shared_configuration → supervisor_configuration → IO (Progress result)) →
    (shared_configuration → supervisor_configuration → NetworkControllerMonad result ()) →
    Network (Maybe ((shared_configuration,supervisor_configuration),RunOutcome result))
genericRunVisitor forkVisitorWorkerThread getConfiguration initializeGlobalState constructVisitor getStartingProgress constructManager = do
    configuration ← liftIO $ getConfiguration
    case configuration of
        SupervisorConfiguration{..} → do
            liftIO $ initializeGlobalState shared_configuration
            starting_progress ← liftIO $ getStartingProgress shared_configuration supervisor_configuration
            termination_result ←
                runSupervisorWithStartingProgress
                    (flip send shared_configuration)
                    default_network_callbacks
                    (unwrapPortID supervisor_port)
                    starting_progress
                    (constructManager shared_configuration supervisor_configuration)
            return $ Just ((shared_configuration,supervisor_configuration),termination_result)
        WorkerConfiguration{..} → do
            handle ← liftIO $ connectTo supervisor_host_name (unwrapPortID supervisor_port)
            shared_configuration ← liftIO $ receive handle
            genericRunWorkerUsingHandle (flip forkVisitorWorkerThread . constructVisitor $ shared_configuration) handle
            return Nothing
-- }}}

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
genericRunWorker spawnWorker host_name port_id =
    liftIO (connectTo host_name port_id)
    >>=
    genericRunWorkerUsingHandle spawnWorker
-- }}}

genericRunWorkerUsingHandle :: -- {{{
    (Monoid result, Serialize result) ⇒
    (
        (WorkerTerminationReason result → IO ()) →
        Workload →
        IO (WorkerEnvironment result)
    ) →
    Handle →
    Network ()
genericRunWorkerUsingHandle spawnWorker handle = liftIO $
    Process.runWorkerUsingHandles handle handle spawnWorker
-- }}}
-- }}}
