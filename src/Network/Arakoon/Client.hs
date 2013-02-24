module Network.Arakoon.Client (
      ClusterId, ProtocolVersion, ClientId, Error(..), NodeName, Key, Value
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
    ) where

import Data.Word
import Data.Serialize (Result(..), runGetPartial, runPutLazy)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as LBS

import Control.Monad.IO.Class (MonadIO, liftIO)

import Network.Socket (Socket)
import qualified Network.Socket.ByteString as S
import qualified Network.Socket.ByteString.Lazy as LS

import Network.Arakoon.Types
import Network.Arakoon.Protocol
import Network.Arakoon.Serialize hiding (get)

sendPrologue :: MonadIO m => Socket -> ClusterId -> ProtocolVersion -> m ()
sendPrologue s n v = liftIO $ do
    let p = runPutLazy $ prologue n v
    LS.sendAll s p

runCommand :: (Response a, MonadIO m) => Socket -> Command a -> m (Either Error a)
runCommand s c = liftIO $ do
    LS.sendAll s $ runPutLazy $ putCommand c
    loop $ Partial $ runGetPartial getResponse
  where
    loop p = case p of
        Partial g -> do
            d <- S.recv s 4096
            if BS.null d
                then error "socket closed"
                else loop $ g d
        Done r _ -> return r
        Fail e -> return $ Left $ ClientParseError e
{-# INLINE runCommand #-}

ping :: MonadIO m
     => Socket
     -> ClientId
     -> ClusterId
     -> m (Either Error LBS.ByteString)
ping = command2 Ping

whoMaster :: MonadIO m
          => Socket
          -> m (Either Error (Maybe NodeName))
whoMaster = command0 WhoMaster

get :: MonadIO m
    => Socket
    -> Bool
    -> Key
    -> m (Either Error Value)
get = command2 Get

set :: MonadIO m
    => Socket
    -> Key
    -> Value
    -> m (Either Error ())
set = command2 Set

exists :: MonadIO m
       => Socket
       -> Bool
       -> Key
       -> m (Either Error Bool)
exists = command2 Exists

expectProgressPossible :: MonadIO m
                       => Socket
                       -> m (Either Error Bool)
expectProgressPossible = command0 ExpectProgressPossible

multiGet :: MonadIO m
         => Socket
         -> Bool
         -> [Key]
         -> m (Either Error [Value])
multiGet = command2 MultiGet

delete :: MonadIO m
       => Socket
       -> Key
       -> m (Either Error ())
delete = command1 Delete

range :: MonadIO m
      => Socket
      -> Bool
      -> Maybe Key
      -> Bool
      -> Maybe Key
      -> Bool
      -> Word32
      -> m (Either Error [Key])
range = command6 Range

rangeEntries :: MonadIO m
             => Socket
             -> Bool
             -> Maybe Key
             -> Bool
             -> Maybe Key
             -> Bool
             -> Word32
             -> m (Either Error [(Key, Value)])
rangeEntries = command6 RangeEntries

prefix :: MonadIO m
       => Socket
       -> Bool
       -> Key
       -> Word32
       -> m (Either Error [Key])
prefix = command3 Prefix

testAndSet :: MonadIO m
           => Socket
           -> Key
           -> Maybe Value
           -> Maybe Value
           -> m (Either Error (Maybe Value))
testAndSet = command3 TestAndSet

revRangeEntries :: MonadIO m
                => Socket
                -> Bool
                -> Maybe Key
                -> Bool
                -> Maybe Key
                -> Bool
                -> Word32
                -> m (Either Error [(Key, Value)])
revRangeEntries = command6 RevRangeEntries

assertExists :: MonadIO m
             => Socket
             -> Bool
             -> Key
             -> m (Either Error ())
assertExists = command2 AssertExists

deletePrefix :: MonadIO m
             => Socket
             -> Key
             -> m (Either Error Word32)
deletePrefix = command1 DeletePrefix


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
command5 :: (Response a, MonadIO m)
         => (a1 -> a2 -> a3 -> a4 -> a5 -> Command a)
         -> Socket
         -> a1 -> a2 -> a3 -> a4 -> a5
         -> m (Either Error a)
command5 c s a1 a2 a3 a4 a5 = runCommand s $ c a1 a2 a3 a4 a5
-}
command6 :: (Response a, MonadIO m)
         => (a1 -> a2 -> a3 -> a4 -> a5 -> a6 -> Command a)
         -> Socket
         -> a1 -> a2 -> a3 -> a4 -> a5 -> a6
         -> m (Either Error a)
command6 c s a1 a2 a3 a4 a5 a6 = runCommand s $ c a1 a2 a3 a4 a5 a6
{-# INLINE command6 #-}
