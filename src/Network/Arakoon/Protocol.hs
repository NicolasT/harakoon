module Network.Arakoon.Protocol (
      prologue
    , getResponse
    , Response(..)
    ) where

import Data.Serialize hiding (get)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as LBS

import Network.Arakoon.Types
import Network.Arakoon.Protocol.Utils

prologue :: ClusterId -> ProtocolVersion -> Put
prologue n v = do
    putCommandId 0x00
    putWord32le v
    putBS n
{-# INLINE prologue #-}

getResponse :: Response a => Get (Either Error a)
getResponse = do
    rc <- getWord32le
    case rc of
        0 -> Right `fmap` get
        n -> (Left . parseError n) `fmap` get
{-# INLINE getResponse #-}

class Response a where
    get :: Get a

instance Response LBS.ByteString where
    get = getWord32le >>= getLazyByteString . fromIntegral
    {-# INLINE get #-}

instance Response BS.ByteString where
    get = getWord32le >>= getByteString . fromIntegral
    {-# INLINE get #-}

instance Response () where
    get = return ()
    {-# INLINE get #-}

instance Response a => Response (Maybe a) where
    get = do
        tag <- getWord8
        case tag of
            0 -> return Nothing
            _ -> Just `fmap` get
    {-# INLINE get #-}
