package main

import (
	"bytes"
	"encoding/base64"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-resty/resty/v2"
	"github.com/joho/godotenv"
	"github.com/libsv/go-bt"
	"github.com/ordishs/go-bitcoin"
)

var bit *bitcoin.Bitcoind

const QUEUE_LENGTH = 10000
const CONCURRENCY = 16

const API = "https://bsv.fyxgaming.com"

// const API = "http://localhost:8080"

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
	queue := make(chan *txn, QUEUE_LENGTH)
	go func() {
		var seq uint64
		for {
			if len(txns) > QUEUE_LENGTH {
				log.Println("AT CAPACITY. SLEEP")
				time.Sleep(time.Second)
				continue
			}
			newSeq := loadQueue(seq, queue)
			if newSeq == seq {
				log.Println("NO ROWS. SLEEP")
				time.Sleep(5 * time.Second)
			}
			seq = newSeq
		}
	}()

	// inFlight := 0
	batchCount := 0

	workers := make(chan bool, CONCURRENCY)
	for {
		tx := <-queue
		// log.Println("PROCESSING:", tx.Tx.GetTxID())
		workers <- true

		go func(tx *txn) {
			rawtx := tx.Tx.ToString()
			txid, err := bit.SendRawTransaction(rawtx)
			if err != nil {
				txid = tx.Tx.GetTxID()
				if !strings.Contains(err.Error(), "Transaction already in the mempool") &&
					!strings.Contains(err.Error(), "txn-already-known") {
					log.Println("ERROR:", txid, len(rawtx)/2, err.Error())
					queue <- tx
					<-workers
					return
				}
				batchCount++
				log.Println("SUCCESS:", txid, len(rawtx)/2, "txn-already-known")
			} else {
				log.Println("SUCCESS:", txid, len(rawtx)/2)
				batchCount++
			}

			// log.Println("Updating record")
			m.Lock()
			toQueue := make([]*txn, 0)
			for childid := range children[txid] {
				child := txns[childid]
				delete(child.Parents, txid)
				// log.Println("Child:", txid, child.Tx.GetTxID(), len(child.Parents))
				if len(child.Parents) == 0 {
					toQueue = append(toQueue, &child)
					// 	queue <- &child
				}
			}
			delete(txns, txid)
			delete(children, txid)
			m.Unlock()
			for _, child := range toQueue {
				log.Println("Queuing Child:", txid, child.Tx.GetTxID(), len(child.Parents))
				queue <- child
			}
			<-workers
		}(tx)
	}
}

func loadQueue(seq uint64, queue chan *txn) uint64 {
	log.Println("LOAD PAGE")
	client := resty.New()
	resp, err := client.R().Get(fmt.Sprintf("%s/txsync/%d", API, seq))
	if err != nil {
		log.Println("Get Batch Error:", err)
		return seq
	}

	r := csv.NewReader(bytes.NewReader(resp.Body()))
	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Println("CSV Read Error:", err)
			break
		}
		seq, err = strconv.ParseUint(record[0], 10, 64)
		if err != nil {
			log.Println("Parse Seq Error:", err)
			break
		}
		tx := txn{
			Parents: make(map[string]bool, 0),
		}

		txbuf, err := base64.StdEncoding.DecodeString(record[1])
		if err != nil {
			log.Println("DecodeString Error:", err)
			break
		}
		tx.Tx, err = bt.NewTxFromBytes(txbuf)
		if err != nil {
			log.Println("Parse Txn Error:", err)
			break
		}
		txid := tx.Tx.GetTxID()
		m.Lock()
		children[txid] = make(map[string]bool)
		for _, txin := range tx.Tx.Inputs {
			if _, ok := txns[txin.PreviousTxID]; ok {
				children[txin.PreviousTxID][txid] = true
				tx.Parents[txin.PreviousTxID] = true
			}
		}
		txns[txid] = tx
		m.Unlock()
		if len(tx.Parents) == 0 {
			queue <- &tx
		}
	}
	return seq
}
