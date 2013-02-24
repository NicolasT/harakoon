{-# LANGUAGE GADTs, StandaloneDeriving #-}
module Network.Arakoon.Types (
      NodeName
    , Key
    , Value
    , ClusterId
    , ClientId
    , ProtocolVersion
    , CommandId
    , Command(..)
    , putCommand
    , Error(..)
    , parseError
    ) where

import Data.Bits
import Data.Word
import Data.Serialize (Putter, putWord32le)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as LBS

import Network.Arakoon.Serialize

type NodeName = BS.ByteString
type Key = LBS.ByteString
type Value = LBS.ByteString
type ClusterId = BS.ByteString
type ClientId = BS.ByteString
type ProtocolVersion = Word32

newtype CommandId = CommandId Word16
  deriving (Show, Read, Eq)
instance Num CommandId where
    fromInteger = CommandId . fromInteger
    (+) = undefined
    (*) = undefined
    abs = undefined
    signum = undefined

instance Argument CommandId where
    put (CommandId c) = putWord32le $ fromIntegral c .|. mask
      where
        mask = 0xb1ff0000
        {-# INLINE mask #-}
    {-# INLINE put #-}

data Command a where
    Ping :: ClientId -> ClusterId -> Command LBS.ByteString
    WhoMaster :: Command (Maybe NodeName)
    Get :: Bool -> Key -> Command Value
    Set :: Key -> Value -> Command ()
    Exists :: Bool -> Key -> Command Bool

deriving instance Show (Command a)
deriving instance Eq (Command a)

putCommand :: Putter (Command a)
putCommand c = case c of
    Ping a b -> putC 0x01 >> put a >> put b
    WhoMaster -> putC 0x02
    Get d k -> putC 0x08 >> put d >> put k
    Set k v -> putC 0x09 >> put k >> put v
    Exists d k -> putC 0x07 >> put d >> put k
  where
    putC :: Putter CommandId
    putC = put
    {-# INLINE putC #-}

data Error = Success
           | NoMagic LBS.ByteString
           | TooManyDeadNodes LBS.ByteString
           | NoHello LBS.ByteString
           | NotMaster LBS.ByteString
           | NotFound LBS.ByteString
           | WrongCluster LBS.ByteString
           | AssertionFailed LBS.ByteString
           | ReadOnly LBS.ByteString
           | NurseryRangeError LBS.ByteString
           | Unknown LBS.ByteString
           | ClientParseError String
  deriving (Show, Read, Eq)

parseError :: Word32 -> LBS.ByteString -> Error
parseError t v = case t of
    0x00 -> Success
    0x01 -> NoMagic v
    0x02 -> TooManyDeadNodes v
    0x03 -> NoHello v
    0x04 -> NotMaster v
    0x05 -> NotFound v
    0x06 -> WrongCluster v
    0x07 -> AssertionFailed v
    0x08 -> ReadOnly v
    0x09 -> NurseryRangeError v
    _ -> Unknown v
