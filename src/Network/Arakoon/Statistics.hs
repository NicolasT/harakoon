module Network.Arakoon.Statistics (
      FieldName
    , FieldValue(..)
    , NodeStatistics(..)
    ) where

import Data.Int
import Data.Binary.Get
import Data.Binary.IEEE754
import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as BS8
import qualified Data.ByteString.Lazy as LBS

import Network.Arakoon.Serialize

type FieldName = BS.ByteString
data FieldValue = FVInt32 Int32
                | FVInt64 Int64
                | FVFloat Double
                | FVString LBS.ByteString
                | FVList [(FieldName, FieldValue)]
  deriving (Show, Eq)
newtype NodeStatistics = NodeStatistics [(FieldName, FieldValue)]
  deriving (Show)

instance Response NodeStatistics where
    get = do
        s <- get
        case runGetOrFail parseList s of
            Left (_, _, e) -> fail e
            Right (_, _, l) ->
                case lookup (BS8.pack "arakoon_stats") l of
                    Just r -> case r of
                        FVList l' -> return $ NodeStatistics l'
                        _ -> fail "NodeStatistics: unexpected type"
                    Nothing -> fail "NodeStatistics: missing field \"arakoon_statistics\""
      where
        parseList = do
            t <- getWord32le
            n <- get

            v <- case t of
                1 -> FVInt32 `fmap` fromIntegral `fmap` getWord32le
                2 -> FVInt64 `fmap` fromIntegral `fmap` getWord64le
                3 -> FVFloat `fmap` getFloat64le
                4 -> FVString `fmap` get
                5 -> do
                    l <- getWord32le
                    let loop acc cnt = case cnt of
                            0 -> return $ FVList acc
                            cnt' -> do
                                f <- parseList
                                loop (f ++ acc) (cnt' - 1)
                    loop [] l
                _ -> fail "NodeStatistics: unexpected type tag"

            return [(n, v)]


