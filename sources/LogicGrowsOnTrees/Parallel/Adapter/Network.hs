{-# LANGUAGE CPP #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UnicodeSyntax #-}

{-| This adapter implements parallelism by allowing multiple workers to connect
    to a supervisor over the network.  For this adapter, workers are started
    separately from the supervisor, so the number of workers is not set by the
    controller but by the number of workers that connect to supervisor.
 -}
module LogicGrowsOnTrees.Parallel.Adapter.Network
    (
    -- * Driver
      driver
    , driverNetwork
    -- * Network
    , Network(..)
    , runNetwork
    -- * Controller
    , NetworkRequestQueueMonad(..)
    , NetworkControllerMonad
    , abort
    , fork
    , getCurrentProgressAsync
    , getCurrentProgress
    , getCurrentStatisticsAsync
    , getCurrentStatistics
    , getNumberOfWorkersAsync
    , getNumberOfWorkers
    , requestProgressUpdateAsync
    , requestProgressUpdate
    , setWorkloadBufferSize
    -- * Outcome types
    , RunOutcome(..)
    , RunStatistics(..)
    , TerminationReason(..)
    -- * Miscellaneous types
    , NetworkCallbacks(..)
    , default_network_callbacks
    , NetworkConfiguration(..)
    , WorkerId(..)
    , WrappedPortID(..)
    -- * Generic runner functions
    -- $runners
    , runSupervisor
    , runWorker
    , runExplorer
    -- * Utility functions
    , showPortID
    , getConfiguration
    ) where

import Prelude hiding (catch)

import Control.Applicative (Applicative(..))
import Control.Concurrent (ThreadId,forkIO,killThread,myThreadId,throwTo)
import Control.Exception (AsyncException(..),SomeException,bracket,catch,fromException)
import Control.Lens (use)
import Control.Lens.Operators ((%=))
import Control.Lens.TH (makeLenses)
import Control.Monad (forever,when)
import Control.Monad.CatchIO (MonadCatchIO)
import Control.Monad.IO.Class (MonadIO(..))
import Control.Monad.Trans.Reader (ask)
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

import System.Console.CmdTheLine
import System.IO (Handle)
import qualified System.Log.Logger as Logger
import System.Log.Logger (Priority(DEBUG,INFO,NOTICE))
import System.Log.Logger.TH

import Text.PrettyPrint (text)

import LogicGrowsOnTrees
import LogicGrowsOnTrees.Parallel.Common.Message
import qualified LogicGrowsOnTrees.Parallel.Common.Process as Process
import LogicGrowsOnTrees.Parallel.Common.RequestQueue
import LogicGrowsOnTrees.Parallel.Common.Supervisor
    hiding
    (runSupervisor
    ,getCurrentProgress
    ,getCurrentStatistics
    ,getNumberOfWorkers
    ,setWorkloadBufferSize
    )
import LogicGrowsOnTrees.Parallel.ExplorationMode
import LogicGrowsOnTrees.Parallel.Main
import LogicGrowsOnTrees.Parallel.Purity
import LogicGrowsOnTrees.Utils.Handle

--------------------------------------------------------------------------------
----------------------------------- Loggers ------------------------------------
--------------------------------------------------------------------------------

deriveLoggers "Logger" [DEBUG,INFO,NOTICE]

--------------------------------------------------------------------------------
----------------------------------- Network ------------------------------------
--------------------------------------------------------------------------------

{-| This monad exists due to the quirk that on Windows one needs to initialize
    the network system before using it via. 'withSocketsDo'; to ensure that this
    happens, all computations that use the network are run in the 'Network'
    monad which itself is then run using the 'runNetwork' function that is
    equivalent to calling 'withSocketsDo'.
 -}
newtype Network α = Network { unsafeRunNetwork :: IO α }
  deriving (Applicative,Functor,Monad,MonadIO)

{-| Initializes the network subsystem where required (e.g., on Windows). -}
runNetwork :: Network α → IO α
runNetwork = withSocketsDo . unsafeRunNetwork

--------------------------------------------------------------------------------
---------------------------- Mostly internal types -----------------------------
--------------------------------------------------------------------------------

{- NOTE:  These types have been placed here so that they can be seen by the
          following declaration, as use of Template Haskell means that
          declaration order now matters.
 -}

{-| The ID of a worker. -}
data WorkerId = WorkerId
    {   workerHostName :: HostName {-^ the address of the worker -}
    ,   workerPortNumber :: PortNumber {-^ the port number of the worker -}
    } deriving (Eq,Ord,Show,Typeable)

data Worker = Worker
    {   workerHandle :: Handle
    ,   workerThreadId :: ThreadId
    } deriving (Eq,Show)

data NetworkState = NetworkState
    {   _pending_add :: !(Set WorkerId)
    ,   _pending_quit :: !(Set WorkerId)
    ,   _workers :: !(Map WorkerId Worker)
    }
makeLenses ''NetworkState

type NetworkStateMonad = StateT NetworkState IO

--------------------------------------------------------------------------------
---------------------------------- Controller ----------------------------------
--------------------------------------------------------------------------------

{-| This class extends 'RequestQueueMonad' with the ability to forcibly
    disconnect a worker.
 -}
class RequestQueueMonad m ⇒ NetworkRequestQueueMonad m where
    {-| Forcibly disconnects the given worker; calling this function with the
        `WorkerId` of a worker that is no longer connected to the system is
        *not* an error; in that case, nothing will happen.
     -}
    disconnectWorker :: WorkerId → m ()

{-| This is the monad in which the network controller will run. -}
newtype NetworkControllerMonad exploration_mode α =
  C (RequestQueueReader exploration_mode WorkerId NetworkStateMonad α)
  deriving (Applicative,Functor,Monad,MonadCatchIO,MonadIO,RequestQueueMonad)

instance HasExplorationMode (NetworkControllerMonad exploration_mode) where
    type ExplorationModeFor (NetworkControllerMonad exploration_mode) = exploration_mode

instance NetworkRequestQueueMonad (NetworkControllerMonad result) where
    disconnectWorker worker_id = C $ ask >>= (enqueueRequest $ do
        debugM ("Disconnecting worker " ++ show worker_id)
        Map.lookup worker_id <$> use workers >>=
            maybe (pending_add %= Set.delete worker_id)
                  (liftIO . flip send QuitWorker . workerHandle)
     )

--------------------------------------------------------------------------------
--------------------------------- Other Types ----------------------------------
--------------------------------------------------------------------------------

{-| Callbacks used to to notify when a worker has conneted or disconnected. -}
data NetworkCallbacks = NetworkCallbacks
    {   notifyConnected :: WorkerId → IO Bool
            {-^ callback used to notify that a worker is about to connect;
                return 'True' to allow the connection to proceed and 'False'
                to veto it
             -}
    ,   notifyDisconnected :: WorkerId → IO ()
            {-^ callback used to notify that a worker has disconnected -}
    }

{-| A default set of callbacks for when you don't care about being notified of connections and disconnections. -}
default_network_callbacks :: NetworkCallbacks
default_network_callbacks = NetworkCallbacks
    {   notifyConnected = const (return True)
    ,   notifyDisconnected = const (return ())
    }

{-| Configuration information that indicates whether a process should be run in
    supervisor or worker mode.
 -}
data NetworkConfiguration shared_configuration supervisor_configuration =
    {-| This constructor indicates that the process should run in supervisor mode. -} 
    SupervisorConfiguration
    {   shared_configuration :: shared_configuration {-^ configuration information shared between the supervisor and the worker -}
    ,   supervisor_configuration :: supervisor_configuration {-^ configuration information specific to the supervisor -}
    ,   supervisor_port :: WrappedPortID {-^ - for the supervisor, the port on which to listen -}
    }
    {-| This constructor indicates that the process should be run in worker mode. -}
  | WorkerConfiguration
    {   supervisor_host_name :: HostName {-^ the address of the supervisor to which this worker should connect -}
    ,   supervisor_port :: WrappedPortID {-^ - for the worker, the port to which to connect -}
    }

{-| A newtype wrapper around PortID in order to provide an instance of 'ArgVal'. -}
newtype WrappedPortID = WrappedPortID { unwrapPortID :: PortID }

instance ArgVal WrappedPortID where
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

--------------------------------------------------------------------------------
------------------------------- Generic runners --------------------------------
--------------------------------------------------------------------------------

{- $runners
In this section the full functionality of this module is exposed in case one
does not want the restrictions of the driver interface. If you decide to go in
this direction, then you need to decide whether you want there to be a single
executable for both the supervisor and worker with the process of determining in
which mode it should run taken care of for you, or whether you want to do this
yourself in order to give yourself more control (such as by having separate
supervisor and worker executables) at the price of more work.

If you want to use a single executable with automated handling of the
supervisor and worker roles, then use 'runExplorer'.  Otherwise, use
'runSupervisor' to run the supervisor loop and on each worker use 'runWorker'.
 -}

{-| This runs the supervisor, which will listen for connecting workers. -}
runSupervisor ::
    ∀ exploration_mode.
    ( Serialize (ProgressFor exploration_mode)
    , Serialize (WorkerFinishedProgressFor exploration_mode)
    ) ⇒
    ExplorationMode exploration_mode {-^ the exploration mode -} →
    (Handle → IO ()) {-^ an action that writes any information needed by the worker to the given handle -} →
    NetworkCallbacks {-^ callbacks used to signal when a worker has connected or disconnected;  the connect callback has the ability to veto a worker from connecting -} →
    PortID {-^ the port id on which to listen for connections -} →
    ProgressFor exploration_mode {-^ the initial progress of the run -} →
    NetworkControllerMonad exploration_mode () {-^ the controller of the supervisor -} →
    Network (RunOutcomeFor exploration_mode) {-^ the outcome of the run -}
runSupervisor
    exploration_mode
    initializeWorker
    NetworkCallbacks{..}
    port_id
    starting_progress
    (C controller)
 = liftIO $ do
    request_queue ← newRequestQueue

    let receiveStolenWorkloadFromWorker = flip enqueueRequest request_queue .* receiveStolenWorkload

        receiveProgressUpdateFromWorker = flip enqueueRequest request_queue .* receiveProgressUpdate

        receiveFailureFromWorker = flip enqueueRequest request_queue .* receiveWorkerFailure

        receiveFinishedFromWorker worker_id final_progress = flip enqueueRequest request_queue $ do
            removal_flag ← Set.member worker_id <$> use pending_quit
            infoM $ if removal_flag
                then "Worker " ++ show worker_id ++ " has finished, and will be removed."
                else "Worker " ++ show worker_id ++ " has finished, and will look for another workload."
            receiveWorkerFinishedWithRemovalFlag removal_flag worker_id final_progress

        receiveQuitFromWorker worker_id = flip enqueueRequest request_queue $ do
            infoM $ "Worker " ++ show worker_id ++ " has quit."
            pending_quit %= Set.delete worker_id
            workers %= Map.delete worker_id
            removeWorkerIfPresent worker_id
            liftIO $ notifyDisconnected_ worker_id

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

        notifyDisconnected_ worker_id@WorkerId{..} = do
            noticeM $ "Worker " ++ workerHostName ++ ":" ++ show workerPortNumber ++ " has disconnected"
            notifyDisconnected worker_id

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
                    flip enqueueRequestAndWait request_queue $ pending_add %= Set.insert worker_id
                    allowed_to_connect ← liftIO $ notifyConnected worker_id
                    if allowed_to_connect
                        then 
                         do debugM $ identifier ++ " is allowed to connect."
                            noticeM $ "Received connection from " ++ identifier
                            initializeWorker workerHandle
                            workerThreadId ← forkIO (
                                receiveAndProcessMessagesFromWorkerUsingHandle
                                    (MessageForSupervisorReceivers{..} :: MessageForSupervisorReceivers exploration_mode WorkerId)
                                    workerHandle
                                    worker_id
                                `catch`
                                (\e → case fromException e of
                                    Just ThreadKilled → sendToWorker QuitWorker
                                    Just UserInterrupt → sendToWorker QuitWorker
                                    _ → do enqueueRequest (removeWorker worker_id) request_queue
                                           sendToWorker QuitWorker `catch` (\(_::SomeException) → return ())
                                           liftIO $ notifyDisconnected_ worker_id
                                )
                             )
                            flip enqueueRequest request_queue $ do
                                is_pending_add ← Set.member worker_id <$> use pending_add
                                when is_pending_add $ do
                                    pending_add %= Set.delete worker_id
                                    workers %= Map.insert worker_id Worker{..}
                                    addWorker worker_id
                        else
                         do debugM $ identifier ++ " is *not* allowed to connect."
                            noticeM $ "Rejected connections from " ++ identifier
                            sendToWorker QuitWorker
            )
        `catch`
        (\e → case fromException e of
            Just ThreadKilled → return ()
            _ → throwTo supervisor_thread_id e
        )
    forkControllerThread request_queue controller
    flip evalStateT (NetworkState mempty mempty mempty) $ do
        supervisor_outcome@SupervisorOutcome{supervisorRemainingWorkers} ←
            runSupervisorStartingFrom
                exploration_mode
                starting_progress
                SupervisorCallbacks{..}
                (requestQueueProgram (return ()) request_queue)
        broadcastMessageToWorkers QuitWorker supervisorRemainingWorkers
        liftIO $ killThread acceptor_thread_id
        killControllerThreads request_queue
        return $ extractRunOutcomeFromSupervisorOutcome supervisor_outcome
{-# INLINE runSupervisor #-}

{-| Explores the given tree using multiple processes to achieve parallelism.

    This function grants access to all of the functionality of this adapter,
    rather than having to go through the more restricted driver interface. The
    signature of this function is very complicated because it is meant to be
    used in both the supervisor and worker.  The configuration information is
    used to determine whether the program is being run in supervisor mode or in
    worker mode;  in the former case, the configuration is further split into
    configuration information that is shared between the supervisor and the
    worker and configuration information that is specific to the supervisor.
 -}
runExplorer ::
    ( Serialize shared_configuration
    , Serialize (ProgressFor exploration_mode)
    , Serialize (WorkerFinishedProgressFor exploration_mode)
    ) ⇒
    (shared_configuration → ExplorationMode exploration_mode)
        {-^ a function that constructs the exploration mode given the shared
            configuration
         -} →
    Purity m n {-^ the purity of the tree -} →
    IO (NetworkConfiguration shared_configuration supervisor_configuration)
        {-^ an action that gets the configuration information (run on both
            supervisor and worker processes); this also determines whether we
            are in supervisor or worker mode based on whether the constructor
            use is respectively 'SupervisorConfiguration' or
            'WorkerConfiguration'
         -} →
    (shared_configuration → IO ())
        {-^ an action that initializes the global state of the process given the
            shared configuration (run on both supervisor and worker processes)
         -} →
    (shared_configuration → TreeT m (ResultFor exploration_mode))
        {-^ a function that constructs the tree from the shared configuration
            (called only on the worker)
         -} →
    (shared_configuration → supervisor_configuration → IO (ProgressFor exploration_mode))
        {-^ an action that gets the starting progress given the full
            configuration information (run only on the supervisor)
         -} →
    (shared_configuration → supervisor_configuration → NetworkControllerMonad exploration_mode ())
        {-^ a function that constructs the controller for the supervisor (called
            only on the supervisor)
         -} →
    Network (Maybe ((shared_configuration,supervisor_configuration),RunOutcomeFor exploration_mode))
        {-^ if this process is the supervisor, then the outcome of the run as
            well as the configuration information wrapped in 'Just'; otherwise
            'Nothing'
         -}
runExplorer
    constructExplorationMode
    purity
    getConfiguration
    initializeGlobalState
    constructTree
    getStartingProgress
    constructController
 = do
    configuration ← liftIO $ getConfiguration
    case configuration of
        SupervisorConfiguration{..} → do
            liftIO $ initializeGlobalState shared_configuration
            starting_progress ← liftIO $ getStartingProgress shared_configuration supervisor_configuration
            termination_result ←
                runSupervisor
                    (constructExplorationMode shared_configuration)
                    (flip send shared_configuration)
                    default_network_callbacks
                    (unwrapPortID supervisor_port)
                    starting_progress
                    (constructController shared_configuration supervisor_configuration)
            return $ Just ((shared_configuration,supervisor_configuration),termination_result)
        WorkerConfiguration{..} → liftIO $ do
            handle ← connectTo supervisor_host_name (unwrapPortID supervisor_port)
            shared_configuration ← receive handle
            Process.runWorkerUsingHandles
                (constructExplorationMode shared_configuration)
                purity
                (constructTree shared_configuration)
                handle
                handle
            return Nothing
{-# INLINE runExplorer #-}

{-| Runs a worker that connects to the supervisor via. the given address and port id. -}
runWorker ::
    ( Serialize (ProgressFor exploration_mode)
    , Serialize (WorkerFinishedProgressFor exploration_mode)
    ) ⇒
    ExplorationMode exploration_mode {-^ the mode in to explore the tree -} →
    Purity m n {-^ the purity of the tree -} →
    TreeT m (ResultFor exploration_mode) {-^ the tree -} →
    HostName {-^ the address of the supervisor -} →
    PortID {-^ the port id on which the supervisor is listening -} →
    Network ()
runWorker exploration_mode purity tree host_name port_id = liftIO $ do
    handle ← connectTo host_name port_id
    Process.runWorkerUsingHandles exploration_mode purity tree handle handle
{-# INLINE runWorker #-}

--------------------------------------------------------------------------------
------------------------------- Utility funtions -------------------------------
--------------------------------------------------------------------------------

{-| Processes the command line and returns the network configuration;  it uses
    the first argument to determine whether the configuration should be for a
    supervisor or for a worker.
 -}
getConfiguration ::
    Term shared_configuration {-^ configuration that is shared between the supervisor and the worker -} →
    Term supervisor_configuration {-^ configuration that is specific to the supervisor -} →
    TermInfo {-^ program information (you should at least set 'termDoc' with the program description) -} →
    IO (NetworkConfiguration shared_configuration supervisor_configuration) {-^ the configuration obtained from the command line -}
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

{-| Constructs a string representation of a port id.  (This function is needed
    if using an older version of the @Network@ package that doesn't have a
    'Show' instance for 'PortID'.)
 -}
showPortID :: PortID → String
showPortID (Service service_name) = "Service " ++ service_name
showPortID (PortNumber port_number) = "Port Number " ++ show port_number
#ifndef mingw32_HOST_OS
showPortID (UnixSocket unix_socket_name) = "Unix Socket " ++ unix_socket_name
#endif

--------------------------------------------------------------------------------
------------------------------------ Driver ------------------------------------
--------------------------------------------------------------------------------

{- NOTE:  This section is last so that it can see everything before it, as my
          use of Template Haskell seems to have disturbed the ability of GHC to
          see declarations out of order.
 -}

{-| This is the driver for the network adapter;  it consists of a supervisor
    that listens for connections and multiple workers that connect to the
    supervisor, where the same executable is used for both the supervisor and
    the worker. To start the supervisor, run the executable with "supervisor" as
    the first argument and "-p PORTID" to specify the port id. To start a
    worker, run the executable with "worker" as the first argument, the address
    of the supervisor as the second, and the port id as the third.
 -}
driver ::
    ∀ shared_configuration supervisor_configuration m n exploration_mode.
    ( Serialize shared_configuration
    , Serialize (ProgressFor exploration_mode)
    , Serialize (WorkerFinishedProgressFor exploration_mode)
    ) ⇒
    Driver IO shared_configuration supervisor_configuration m n exploration_mode
driver =
    case (driverNetwork :: Driver Network shared_configuration supervisor_configuration m n exploration_mode) of
        Driver runDriver → Driver (runNetwork . runDriver)
{-# INLINE driver #-}

{-| This is the same as 'driver', but runs in the 'Network' monad.  Use this
    driver if you want to do other things with the network (such as starting
    a subseqent parallel exploration) after the run completes.
 -}
driverNetwork ::
    ∀ shared_configuration supervisor_configuration m n exploration_mode.
    ( Serialize shared_configuration
    , Serialize (ProgressFor exploration_mode)
    , Serialize (WorkerFinishedProgressFor exploration_mode)
    ) ⇒
    Driver Network shared_configuration supervisor_configuration m n exploration_mode
driverNetwork = Driver $ \DriverParameters{..} → do
    runExplorer
        constructExplorationMode
        purity
        (getConfiguration shared_configuration_term supervisor_configuration_term program_info)
        initializeGlobalState
        constructTree
        getStartingProgress
        constructController
    >>=
    maybe (return ()) (liftIO . (notifyTerminated <$> fst . fst <*> snd . fst <*> snd))
{-# INLINE driverNetwork #-}

--------------------------------------------------------------------------------
----------------------------------- Internal -----------------------------------
--------------------------------------------------------------------------------

fromJustOrBust message = fromMaybe (error message)

no_configuration_term = ret (pure $ helpFail Plain Nothing)

supervisorConfigurationTermFor shared_configuration_term supervisor_configuration_term =
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

worker_configuration_term =
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
