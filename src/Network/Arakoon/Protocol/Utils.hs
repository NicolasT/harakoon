module Network.Arakoon.Protocol.Utils (
      putCommandId
    , putBS
    , putLBS
    ) where

import Data.Bits
import Data.Word
import Data.Serialize
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as LBS

mask :: Word32
mask = 0xb1ff0000
{-# INLINE mask #-}

putCommandId :: Word16 -> Put
putCommandId c = putWord32le $ fromIntegral c .|. mask
{-# INLINE putCommandId #-}

putBS :: BS.ByteString -> Put
putBS s = do
    putWord32le $ fromIntegral $ BS.length s
    putByteString s
{-# INLINE putBS #-}

putLBS :: LBS.ByteString -> Put
putLBS s = do
    putWord32le $ fromIntegral $ LBS.length s
    putLazyByteString s
{-# INLINE putLBS #-}
