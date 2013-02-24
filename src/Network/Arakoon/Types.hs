{-# LANGUAGE GADTs, StandaloneDeriving #-}
module Network.Arakoon.Types (
      NodeName
    , Key
    , Value
    , ClusterId
    , ClientId
    , ProtocolVersion
    , Command(..)
    , putCommand
    , Error(..)
    , parseError
    ) where

import Data.Word
import Data.Serialize
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as LBS

import Network.Arakoon.Protocol.Utils

type NodeName = BS.ByteString
type Key = LBS.ByteString
type Value = LBS.ByteString
type ClusterId = BS.ByteString
type ClientId = BS.ByteString
type ProtocolVersion = Word32

data Command a where
    Ping :: ClientId -> ClusterId -> Command LBS.ByteString
    WhoMaster :: Command (Maybe NodeName)
    Get :: Bool -> Key -> Command Value
    Set :: Key -> Value -> Command ()

deriving instance Show (Command a)
deriving instance Eq (Command a)

putCommand :: Putter (Command a)
putCommand c = case c of
    Ping a b -> putCommandId 0x01 >> putBS a >> putBS b
    WhoMaster -> putCommandId 0x02
    Get d k -> putCommandId 0x08 >> putBool d >> putLBS k
    Set k v -> putCommandId 0x09 >> putLBS k >> putLBS v
  where
    putBool :: Bool -> Put
    putBool b = putWord8 $ if b then 1 else 0
    {-# INLINE putBool #-}

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
