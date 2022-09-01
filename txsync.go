package main

import (
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/donovanhide/eventsource"
	"github.com/go-resty/resty/v2"
	"github.com/joho/godotenv"
	"github.com/libsv/go-bt"
	"github.com/ordishs/go-bitcoin"
)

var bit *bitcoin.Bitcoind

const QUEUE_LENGTH = 10000
const CONCURRENCY = 16

const API = "https://dev.api.fyxgaming.com"

type txn struct {
	Tx      *bt.Tx
	Parents map[string]bool
}

var txns map[string]txn
var children map[string](map[string]bool)
var m sync.Mutex

func init() {
	txns = make(map[string]txn, 0)
	children = make(map[string]map[string]bool, 0)

	godotenv.Load(".env")

	port, err := strconv.ParseInt(os.Getenv("BITCOIN_PORT"), 10, 32)
	bit, err = bitcoin.New(
		os.Getenv("BITCOIN_HOST"),
		int(port),
		os.Getenv("BITCOIN_USER"),
		os.Getenv("BITCOIN_PASS"),
		false)
	if err != nil {
		log.Panicln("Bitcoin RPC Error:", err)
	}
}

func main() {
	go func() {
		stream, err := eventsource.Subscribe(fmt.Sprintf("%s/txsync/sse", API), "")
		if err != nil {
			return
		}
		for {
			ev := <-stream.Events
			processTxn(ev.Data())
		}
	}()
}

func processTxn(hexid string) {
	txhex, err := bit.GetRawTransactionHex(hexid)
	if err == nil || len(*txhex) > 0 {
		log.Println("SKIPPING:", hexid)
		return
	}

	client := resty.New()
	resp, err := client.R().Get(fmt.Sprintf("%s/txsync/tx/%s", API, hexid))
	if err != nil {
		log.Panicf("Fetch Error: %s, %+v\n", hexid, err)
	}
	if resp.StatusCode() >= 400 {
		log.Panicf("Fetch Error: %s, %d\n", hexid, resp.StatusCode())
	}

	rawtx := resp.Body()
	hexid, err = bit.SendRawTransaction(hex.EncodeToString(rawtx))

	if err != nil {
		if !strings.Contains(err.Error(), "Transaction already in the mempool") &&
			!strings.Contains(err.Error(), "txn-already-known") {
			log.Panicln("ERROR:", hexid, err.Error())
			return
		}
		log.Println("SUCCESS:", hexid, len(rawtx)/2, "txn-already-known")
	} else {
		log.Println("SUCCESS:", hexid, len(rawtx)/2)
	}
}
