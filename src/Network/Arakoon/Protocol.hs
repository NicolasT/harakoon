module Network.Arakoon.Protocol (
      prologue
    , getResponse
    ) where

import Data.Binary.Get
import Data.Binary.Put

import Network.Arakoon.Types
import Network.Arakoon.Serialize

prologue :: ClusterId -> ProtocolVersion -> Put
prologue n v = do
    putCommandId (0x00 :: CommandId)
    putWord32le v
    put n
{-# INLINE prologue #-}

getResponse :: Response a => Get (Either Error a)
getResponse = do
    rc <- getWord32le
    case rc of
        0 -> Right `fmap` get
        n -> (Left . parseError n) `fmap` get
{-# INLINE getResponse #-}
