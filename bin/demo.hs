{-# LANGUAGE OverloadedStrings #-}

module Main (main) where

import Data.ByteString.Char8 ()
import Data.ByteString.Lazy.Char8 ()

import Network.Socket

import qualified Network.Arakoon.Client as A

main :: IO ()
main = do
    s <- socket AF_INET Stream defaultProtocol
    a <- inet_addr "127.0.0.1"
    connect s $ SockAddrInet 4000 a
    A.sendPrologue s "arakoon" 1
    rp <- A.ping s "clientid" "arakoon"
    print rp
    rw <- A.whoMaster s
    print rw
    rs <- A.set s "key" "value"
    print rs
    rg <- A.get s False "key"
    print rg
    sClose s
