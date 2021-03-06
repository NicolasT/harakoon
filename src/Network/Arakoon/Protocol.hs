module Network.Arakoon.Protocol (
      prologue
    , getResponse
    ) where

import Data.Monoid
import Data.Binary.Get
import Data.ByteString.Builder

import Network.Arakoon.Types
import Network.Arakoon.Serialize

prologue :: ClusterId -> ProtocolVersion -> Builder
prologue n v = putCommandId (0x00 :: CommandId) <> word32LE v <> put n
{-# INLINE prologue #-}

getResponse :: Response a => Get (Either Error a)
getResponse = do
    rc <- getWord32le
    case rc of
        0 -> Right `fmap` get
        n -> (Left . parseError n) `fmap` get
{-# INLINE getResponse #-}
