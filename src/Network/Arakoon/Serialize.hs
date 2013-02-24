module Network.Arakoon.Serialize (
      Argument(..)
    , Response(..)
    ) where

import Data.Serialize hiding (get, put)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as LBS

class Response a where
    get :: Get a

class Argument a where
    put :: Putter a

instance Response LBS.ByteString where
    get = getWord32le >>= getLazyByteString . fromIntegral
    {-# INLINE get #-}

instance Argument LBS.ByteString where
    put s = putWord32le (fromIntegral $ LBS.length s) >> putLazyByteString s
    {-# INLINE put #-}

instance Response BS.ByteString where
    get = getWord32le >>= getByteString . fromIntegral
    {-# INLINE get #-}

instance Argument BS.ByteString where
    put s = putWord32le (fromIntegral $ BS.length s) >> putByteString s
    {-# INLINE put #-}

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

instance Argument a => Argument (Maybe a) where
    put v = case v of
        Nothing -> putWord8 0
        Just v' -> putWord8 1 >> put v'
    {-# INLINE put #-}

instance Response Bool where
    get = do
        tag <- getWord8
        case tag of
            0 -> return False
            _ -> return True
    {-# INLINE get #-}

instance Argument Bool where
    put b = putWord8 $ if b then 1 else 0
    {-# INLINE put #-}
