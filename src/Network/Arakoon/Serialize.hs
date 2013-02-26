{-# LANGUAGE FlexibleInstances #-}

module Network.Arakoon.Serialize (
      putCommandId
    , Argument(..)
    , Response(..)
    , FoldableBuilder(..)
    ) where

import Prelude hiding (foldr)

import Data.Int
import Data.Bits
import Data.Word
import Data.Monoid
import Data.Foldable (Foldable, foldr)
import Data.Binary.Get
import Data.ByteString.Builder
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as LBS

import Control.Applicative

putCommandId :: Word32 -> Builder
putCommandId c = word32LE $ c .|. mask
  where
    mask :: Word32
    mask = 0xb1ff0000
{-# INLINE putCommandId #-}

class Response a where
    get :: Get a

class Argument a where
    put :: a -> Builder

instance Response LBS.ByteString where
    get = getWord32le >>= getLazyByteString . fromIntegral
    {-# INLINE get #-}

instance Argument LBS.ByteString where
    put s = word32LE (fromIntegral $ LBS.length s) <> lazyByteString s
    {-# INLINE put #-}

instance Response BS.ByteString where
    get = getWord32le >>= getByteString . fromIntegral
    {-# INLINE get #-}

instance Argument BS.ByteString where
    put s = word32LE (fromIntegral $ BS.length s) <> byteString s
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
        Nothing -> word8 0
        Just v' -> word8 1 <> put v'
    {-# INLINE put #-}

instance Response Bool where
    get = do
        tag <- getWord8
        case tag of
            0 -> return False
            _ -> return True
    {-# INLINE get #-}

instance Argument Bool where
    put b = word8 $ if b then 1 else 0
    {-# INLINE put #-}

instance Response Word32 where
    get = getWord32le
    {-# INLINE get #-}

instance Argument Word32 where
    put = word32LE
    {-# INLINE put #-}

instance Response a => Response [a] where
    get = getWord32le >>= loop []
      where
        loop acc c = case c of
            0 -> return acc
            c' -> do
                v <- get
                loop (v : acc) (c' - 1)
    {-# INLINE get #-}

newtype FoldableBuilder f = FoldableBuilder f

instance (Foldable f, Argument a) => Argument (FoldableBuilder (f a)) where
    put (FoldableBuilder l) = word32LE cnt <> s
      where
       (cnt, s) = foldr (\e (c, m) -> (c + 1, put e <> m)) (0, mempty) l
    {-# INLINE put #-}

instance (Response a, Response b) => Response (a, b) where
    get = (,) <$> get <*> get
    {-# INLINE get #-}

instance Argument Int32 where
    put = int32LE
    {-# INLINE put #-}

instance Response Int32 where
    get = fromIntegral `fmap` getWord32le
    {-# INLINE get #-}
