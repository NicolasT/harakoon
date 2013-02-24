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

-- | Identifier of a node
type NodeName = BS.ByteString
-- | Type alias for keys
type Key = LBS.ByteString
-- | Type alias for values
type Value = LBS.ByteString
-- | Identifier of a cluster
type ClusterId = BS.ByteString
-- | Identifier of a client
type ClientId = BS.ByteString
-- | Protocol version
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
    ExpectProgressPossible :: Command Bool
    MultiGet :: Bool -> [Key] -> Command [Value]
    Delete :: Key -> Command ()
    Range :: Bool -> Maybe Key -> Bool -> Maybe Key -> Bool -> Word32 -> Command [Key]
    RangeEntries :: Bool -> Maybe Key -> Bool -> Maybe Key -> Bool -> Word32 -> Command [(Key, Value)]
    Prefix :: Bool -> Key -> Word32 -> Command [Key]
    TestAndSet :: Key -> Maybe Value -> Maybe Value -> Command (Maybe Value)
    RevRangeEntries :: Bool -> Maybe Key -> Bool -> Maybe Key -> Bool -> Word32 -> Command [(Key, Value)]
    AssertExists :: Bool -> Key -> Command ()
    DeletePrefix :: Key -> Command Word32

deriving instance Show (Command a)
deriving instance Eq (Command a)

putCommand :: Putter (Command a)
putCommand c = case c of
    Ping a b -> putC 0x01 >> put a >> put b
    WhoMaster -> putC 0x02
    Get d k -> putC 0x08 >> put d >> put k
    Set k v -> putC 0x09 >> put k >> put v
    Exists d k -> putC 0x07 >> put d >> put k
    ExpectProgressPossible -> putC 0x12
    MultiGet d k -> putC 0x11 >> put d >> put k
    Delete k -> putC 0x0a >> put k
    Range d f fi t ti l -> putC 0x0b >> put d >> put f >> put fi >> put t >> put ti >> put l
    RangeEntries d f fi t ti l -> putC 0x0f >> put d >> put f >> put fi >> put t >> put ti >> put l
    Prefix d k l -> putC 0x0c >> put d >> put k >> put l
    TestAndSet k tv sv -> putC 0x0d >> put k >> put tv >> put sv
    RevRangeEntries d f fi t ti l -> putC 0x23 >> put d >> put f >> put fi >> put t >> put ti >> put l
    AssertExists d k -> putC 0x29 >> put d >> put k
    DeletePrefix k -> putC 0x27 >> put k
  where
    putC :: Putter CommandId
    putC = put
    {-# INLINE putC #-}

-- | Return codes
data Error = Success  -- ^ Success
           | NoMagic LBS.ByteString  -- ^ Magic doesn't match
           | TooManyDeadNodes LBS.ByteString  -- ^ Too many dead nodes
           | NoHello LBS.ByteString  -- ^ Client didn't send a 'hello' command
           | NotMaster LBS.ByteString  -- ^ Node is not master
           | NotFound LBS.ByteString  -- ^ Key not found
           | WrongCluster LBS.ByteString  -- ^ Wrong cluster identifier
           | AssertionFailed LBS.ByteString  -- ^ Assertion failed
           | ReadOnly LBS.ByteString  -- ^ Node is read-only
           | NurseryRangeError LBS.ByteString  -- ^ Request outside nursery range
           | Unknown LBS.ByteString  -- ^ Unknown return code
           | ClientParseError String  -- ^ Response parsing failure
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
