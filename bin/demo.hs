{-# LANGUAGE OverloadedStrings #-}

module Main (main) where

import Control.Exception.Base (bracket)

-- For IsString / OverloadedStrings
import Data.ByteString.Char8 ()
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
