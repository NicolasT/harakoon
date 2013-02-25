module Network.Arakoon.Client (
      ClusterId, ProtocolVersion, ClientId, Error(..), NodeName, Key, Value, VersionInfo(..)
    , sendPrologue
    , ping
    , whoMaster
    , get
    , set
    , exists
    , expectProgressPossible
    , multiGet
    , delete
    , range
    , rangeEntries
    , prefix
    , testAndSet
    , revRangeEntries
    , assertExists
    , deletePrefix
    , version
    ) where

import Data.Word
import Data.Binary.Get
import Data.Binary.Put
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as LBS

import Control.Monad.IO.Class (MonadIO, liftIO)

import Network.Socket (Socket)
import qualified Network.Socket.ByteString as S
import qualified Network.Socket.ByteString.Lazy as LS

import Network.Arakoon.Types
import Network.Arakoon.Protocol
import Network.Arakoon.Serialize hiding (get)

-- | Send the protocol prologue to a node
sendPrologue :: MonadIO m
             => Socket  -- ^ Client socket
             -> ClusterId  -- ^ Identifier of the cluster we expect to be talking to
             -> ProtocolVersion  -- ^ Protocol version
             -> m ()
sendPrologue s n v = liftIO $
    LS.sendAll s $ runPut $ prologue n v

runCommand :: (Response a, MonadIO m)
           => Socket
           -> Command a
           -> m (Either Error a)
runCommand s c = liftIO $ do
    LS.sendAll s $ runPut $ putCommand c
    loop $ runGetIncremental getResponse
  where
    loop p = case p of
        Partial g -> do
            d <- S.recv s 4096
            if BS.null d
                then error "socket closed"
                else loop $ g $ Just d
        Done _ _ r -> return r
        Fail _ _ e -> return $ Left $ ClientParseError e
{-# INLINE runCommand #-}

-- | Send a 'Ping' command to the node
ping :: MonadIO m
     => Socket  -- ^ Client socket
     -> ClientId  -- ^ Client identifier
     -> ClusterId  -- ^ Cluster identifier
     -> m (Either Error LBS.ByteString)
ping = command2 Ping

-- | Send a 'WhoMaster' command to the node
whoMaster :: MonadIO m
          => Socket  -- ^ Client socket
          -> m (Either Error (Maybe NodeName))
whoMaster = command0 WhoMaster

-- | Send a 'Get' command to the node
get :: MonadIO m
    => Socket  -- ^ Client socket
    -> Bool  -- ^ Allow dirty
    -> Key  -- ^ Key to lookup
    -> m (Either Error Value)
get = command2 Get

-- | Send a 'Set' command to the node
set :: MonadIO m
    => Socket  -- ^ Client socket
    -> Key  -- ^ Key to set
    -> Value  -- ^ Value to set
    -> m (Either Error ())
set = command2 Set

-- | Send an 'Exists' command to the node
exists :: MonadIO m
       => Socket  -- ^ Client socket
       -> Bool  -- ^ Allow dirty
       -> Key  -- ^ Key to check
       -> m (Either Error Bool)
exists = command2 Exists

-- | Send an 'ExpectProgressPossible' command to the node
expectProgressPossible :: MonadIO m
                       => Socket  -- ^ Client socket
                       -> m (Either Error Bool)
expectProgressPossible = command0 ExpectProgressPossible

-- | Send a 'MultiGet' command to the node
multiGet :: MonadIO m
         => Socket  -- ^ Client socket
         -> Bool  -- ^ Allow dirty
         -> [Key]  -- ^ Keys to retrieve
         -> m (Either Error [Value])
multiGet = command2 MultiGet

-- | Send a 'Delete' command to the node
delete :: MonadIO m
       => Socket  -- ^ Client socket
       -> Key  -- ^ Key to delete
       -> m (Either Error ())
delete = command1 Delete

-- | Send a 'Range' command to the node
range :: MonadIO m
      => Socket  -- ^ Client socket
      -> Bool  -- ^ Allow dirty
      -> Maybe Key  -- ^ Range begin
      -> Bool  -- ^ Range begin included
      -> Maybe Key  -- ^ Range end
      -> Bool  -- ^ Range end included
      -> Word32  -- ^ Count
      -> m (Either Error [Key])
range = command6 Range

-- | Send a 'RangeEntries' command to the node
rangeEntries :: MonadIO m
             => Socket  -- ^ Client socket
             -> Bool  -- ^ Allow dirty
             -> Maybe Key  -- ^ Range begin
             -> Bool  -- ^ Range begin included
             -> Maybe Key  -- ^ Range end
             -> Bool  -- ^ Range end included
             -> Word32  -- ^ Count
             -> m (Either Error [(Key, Value)])
rangeEntries = command6 RangeEntries

-- | Send a 'Prefix' command to the node
prefix :: MonadIO m
       => Socket  -- ^ Client socket
       -> Bool  -- ^ Allow dirty
       -> Key  -- ^ Prefix
       -> Word32  -- ^ Count
       -> m (Either Error [Key])
prefix = command3 Prefix

-- | Send a 'TestAndSet' command to the node
testAndSet :: MonadIO m
           => Socket  -- ^ Client socket
           -> Key  -- ^ Key
           -> Maybe Value  -- ^ Initial value
           -> Maybe Value  -- ^ New value
           -> m (Either Error (Maybe Value))
testAndSet = command3 TestAndSet

-- | Send a 'RevRangeEntries' command to the node
revRangeEntries :: MonadIO m
                => Socket  -- ^ Client socket
                -> Bool  -- ^ Allow dirty
                -> Maybe Key  -- ^ Range begin
                -> Bool  -- ^ Range begin included
                -> Maybe Key  -- ^ Range end
                -> Bool  -- ^ Range end included
                -> Word32  -- ^ Count
                -> m (Either Error [(Key, Value)])
revRangeEntries = command6 RevRangeEntries

-- | Send an 'AssertExists' command to the node
assertExists :: MonadIO m
             => Socket  -- ^ Client socket
             -> Bool  -- ^ Allow dirty
             -> Key  -- ^ Key
             -> m (Either Error ())
assertExists = command2 AssertExists

-- | Send a 'DeletePrefix' command to the node
deletePrefix :: MonadIO m
             => Socket  -- ^ Client socket
             -> Key  -- ^ Prefix
             -> m (Either Error Word32)
deletePrefix = command1 DeletePrefix

-- | Send a 'Version' command to the node
version :: MonadIO m
        => Socket
        -> m (Either Error VersionInfo)
version = command0 Version

command0 :: (Response a, MonadIO m)
         => Command a
         -> Socket
         -> m (Either Error a)
command0 c s = runCommand s c
{-# INLINE command0 #-}
command1 :: (Response a, MonadIO m)
         => (a1 -> Command a)
         -> Socket
         -> a1
         -> m (Either Error a)
command1 c s a1 = runCommand s $ c a1
{-# INLINE command1 #-}
command2 :: (Response a, MonadIO m)
         => (a1 -> a2 -> Command a)
         -> Socket
         -> a1 -> a2
         -> m (Either Error a)
command2 c s a1 a2 = runCommand s $ c a1 a2
{-# INLINE command2 #-}
command3 :: (Response a, MonadIO m)
         => (a1 -> a2 -> a3 -> Command a)
         -> Socket
         -> a1 -> a2 -> a3
         -> m (Either Error a)
command3 c s a1 a2 a3 = runCommand s $ c a1 a2 a3
{-# INLINE command3 #-}
{-
command4 :: (Response a, MonadIO m)
         => (a1 -> a2 -> a3 -> a4 -> Command a)
         -> Socket
         -> a1 -> a2 -> a3 -> a4
         -> m (Either Error a)
command4 c s a1 a2 a3 a4 = runCommand s $ c a1 a2 a3 a4
{-# INLINE command4 #-}
command5 :: (Response a, MonadIO m)
         => (a1 -> a2 -> a3 -> a4 -> a5 -> Command a)
         -> Socket
         -> a1 -> a2 -> a3 -> a4 -> a5
         -> m (Either Error a)
command5 c s a1 a2 a3 a4 a5 = runCommand s $ c a1 a2 a3 a4 a5
{-# INLINE command5 #-}
-}
command6 :: (Response a, MonadIO m)
         => (a1 -> a2 -> a3 -> a4 -> a5 -> a6 -> Command a)
         -> Socket
         -> a1 -> a2 -> a3 -> a4 -> a5 -> a6
         -> m (Either Error a)
command6 c s a1 a2 a3 a4 a5 a6 = runCommand s $ c a1 a2 a3 a4 a5 a6
{-# INLINE command6 #-}
