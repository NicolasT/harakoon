module Network.Arakoon.Client (
      ClusterId, ProtocolVersion, ClientId, Error(..), NodeName, Key, Value
    , sendPrologue
    , ping
    , whoMaster
    , get
    , set
    ) where

import Data.Serialize hiding (get)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as LBS

import Control.Monad.IO.Class (MonadIO, liftIO)

import Network.Socket (Socket)
import qualified Network.Socket.ByteString as S
import qualified Network.Socket.ByteString.Lazy as LS

import Network.Arakoon.Types
import Network.Arakoon.Protocol hiding (get)

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

ping :: MonadIO m => Socket -> ClientId -> ClusterId -> m (Either Error LBS.ByteString)
ping s a b = runCommand s $ Ping a b
whoMaster :: MonadIO m => Socket -> m (Either Error (Maybe NodeName))
whoMaster s = runCommand s WhoMaster
get :: MonadIO m => Socket -> Bool -> Key -> m (Either Error Value)
get s d k = runCommand s $ Get d k
set :: MonadIO m => Socket -> Key -> Value -> m (Either Error ())
set s k v = runCommand s $ Set k v
