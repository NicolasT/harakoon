{-# LANGUAGE GADTs, StandaloneDeriving #-}
module Network.Arakoon.Types (
      NodeName
    , Key
    , Value
    , ClusterId
    , ClientId
    , ProtocolVersion
    , CommandId
    , VersionInfo(..)
    , Command(..)
    , putCommand
    , Error(..)
    , parseError
    ) where

import Data.Int
import Data.Word
import Data.Binary (Put)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as LBS

import Control.Applicative

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

type CommandId = Word32

data VersionInfo = VersionInfo { versionInfoMajor :: Int32
                               , versionInfoMinor :: Int32
                               , versionInfoPatch :: Int32
                               , versionInfo :: LBS.ByteString
                               }
  deriving (Show)

instance Response VersionInfo where
    get = VersionInfo <$> get <*> get <*> get <*> get

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
    Version :: Command VersionInfo

deriving instance Show (Command a)
deriving instance Eq (Command a)

putCommand :: Command a -> Put
putCommand c = case c of
    Ping a b -> put2 0x01 a b
    WhoMaster -> put0 0x02
    Exists d k -> put2 0x07 d k
    Get d k -> put2 0x08 d k
    Set k v -> put2 0x09 k v
    MultiGet d k -> put2 0x11 d k
    ExpectProgressPossible -> put0 0x12
    Delete k -> put1 0x0a k
    Range d f fi t ti l -> put6 0x0b d f fi t ti l
    Prefix d k l -> put3 0x0c d k l
    TestAndSet k tv sv -> put3 0x0d k tv sv
    RangeEntries d f fi t ti l -> put6 0x0f d f fi t ti l
    RevRangeEntries d f fi t ti l -> put6 0x23 d f fi t ti l
    DeletePrefix k -> put1 0x27 k
    Version -> put0 0x28
    AssertExists d k -> put2 0x29 d k
  where
    putC :: CommandId -> Put
    putC = putCommandId
    {-# INLINE putC #-}
    put0 :: CommandId -> Put
    put0 = putC
    {-# INLINE put0 #-}
    put1 :: Argument a => CommandId -> a -> Put
    put1 i a = putC i >> put a
    {-# INLINE put1 #-}
    put2 :: (Argument a, Argument b) => CommandId -> a -> b -> Put
    put2 i a1 a2 = putC i >> put a1 >> put a2
    {-# INLINE put2 #-}
    put3 :: (Argument a, Argument b, Argument c) => CommandId -> a -> b -> c -> Put
    put3 i a1 a2 a3 = putC i >> put a1 >> put a2 >> put a3
    {-# INLINE put3 #-}
    put6 :: (Argument a, Argument b, Argument c, Argument d, Argument e, Argument f) => CommandId -> a -> b -> c -> d -> e -> f -> Put
    put6 i a1 a2 a3 a4 a5 a6 = putC i >> put a1 >> put a2 >> put a3 >> put a4 >> put a5 >> put a6
    {-# INLINE put6 #-}


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
