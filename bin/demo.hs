{-# LANGUAGE OverloadedStrings #-}

module Main (main) where

import Control.Monad (forM_)
import Control.Exception.Base (bracket)

-- For IsString / OverloadedStrings
import Data.ByteString.Char8 (ByteString, unpack)
import Data.ByteString.Lazy.Char8 ()

import Network.Socket

import qualified Network.Arakoon.Client as A

host :: String
host = "127.0.0.1"
port :: PortNumber
port = 4000
clusterId :: A.ClusterId
clusterId = "arakoon"
protocolVersion :: A.ProtocolVersion
protocolVersion = 1

main :: IO ()
main = withSocketsDo $ do
    addr <- inet_addr host
    bracket
        (socket AF_INET Stream defaultProtocol)
        sClose
        $ \s -> do
            connect s $ SockAddrInet port addr
            A.sendPrologue s clusterId protocolVersion
            putStr "ping: "
            A.ping s "clientid" clusterId >>= print
            putStr "version: "
            A.version s >>= print
            putStr "whoMaster: "
            A.whoMaster s >>= print
            putStr "set: "
            A.set s "key" "value" >>= print
            putStr "get: "
            A.get s False "key" >>= print
            putStr "delete: "
            A.delete s "key" >>= print
            putStr "sequence assertExists (fails): "
            A.sequence s [A.SequenceAssertExists "key"] >>= print
            putStr "syncedSequence: "
            A.syncedSequence s [ A.SequenceAssert "key" Nothing
                               , A.SequenceSet "key" "value"
                               , A.SequenceAssertExists "key"
                               , A.SequenceAssert "key" (Just "value")
                               , A.SequenceDelete "key"
                               , A.SequenceAssert "key" Nothing
                               , A.SequenceSet "key" "value"
                               , A.SequenceSet "key2" "value2"
                               ] >>= print
            putStr "rangeEntries: "
            A.rangeEntries s False (Just "key") True (Just "key3") True (-1) >>= print
            A.sequence s Nothing >>= print
            A.sequence s (Just $ A.SequenceSet "key3" "value3") >>= print
            putStrLn "Statistics:"
            A.statistics s >>= \r -> case r of
                Left{} -> print r
                Right (A.NodeStatistics i) -> forM_ i $ printStatistics "  "

printStatistics :: String -> (ByteString, A.FieldValue) -> IO ()
printStatistics p (n, i) = case i of
    A.FVInt32 v -> putStrLn $ p ++ unpack n ++ ": i32 " ++ show v
    A.FVInt64 v -> putStrLn $ p ++ unpack n ++ ": i64 " ++ show v
    A.FVFloat v -> putStrLn $ p ++ unpack n ++ ": d " ++ show v
    A.FVString v -> putStrLn $ p ++ unpack n ++ ": s " ++ show v
    A.FVList v -> case v of
        [] -> putStrLn $ p ++ unpack n ++ ": []"
        _ -> do
            putStrLn $ p ++ unpack n ++ ":"
            forM_ v $ printStatistics $ p ++ "  "
