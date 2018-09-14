package main

import (
	"bufio"
	"bytes"
	"encoding/hex"
	"fmt"
	"os"

	"github.com/nknorg/nkn/core/ledger"
	tx "github.com/nknorg/nkn/core/transaction"
	"github.com/nknorg/nkn/db"
	"github.com/urfave/cli"
)

func NewExportCommand() *cli.Command {
	return &cli.Command{
		Name:        "export",
		Usage:       "export db items",
		Description: "export db items",
		ArgsUsage:   "[args]",
		Flags: []cli.Flag{
			cli.StringFlag{
				Name:  "path",
				Usage: "the path of db",
			},
			cli.StringFlag{
				Name:  "item",
				Usage: "the prefix of db. include version, currentblockhash, bookkeeper,asset,issued,prepaid, unspent,utxo,transaction,header,blockhash,block",
			},

			cli.StringFlag{
				Name:  "key",
				Usage: "the key of item, hex string",
			},
		},
		Action: exportAction,
		OnUsageError: func(c *cli.Context, err error, isSubcommand bool) error {
			return cli.NewExitError("", 1)
		},
	}
}

func exportAction(c *cli.Context) (err error) {
	if c.NumFlags() < 2 {
		cli.ShowSubcommandHelp(c)
		return nil
	}

	path := c.String("path")
	item := c.String("item")
	keystr := c.String("key")
	key, _ := hex.DecodeString(keystr)

	//TODO 1. txs number in block
	//TODO 2. headerlist

	prefix := []byte{}
	switch item {
	case "version":
		prefix = []byte{byte(db.CFG_Version)}
	case "currentblockhash":
		prefix = []byte{byte(db.SYS_CurrentBlock)}
	case "bookkeeper":
		prefix = []byte{byte(db.SYS_CurrentBookKeeper)}
	case "asset":
		prefix = append([]byte{byte(db.ST_Info)}, key...)
	case "issued":
		prefix = append([]byte{byte(db.ST_QuantityIssued)}, key...)
	case "prepaid":
		prefix = append([]byte{byte(db.ST_Prepaid)}, key...)
	case "unspent":
		prefix = append([]byte{byte(db.IX_Unspent)}, key...)
	case "utxo":
		prefix = append([]byte{byte(db.IX_Unspent_UTXO)}, key...)
	case "transaction":
		prefix = append([]byte{byte(db.DATA_Transaction)}, key...)
	case "header":
		prefix = append([]byte{byte(db.DATA_Header)}, key...)
	case "blockhash":
		prefix = append([]byte{byte(db.DATA_BlockHash)}, key...)
	case "block":
		prefix = append([]byte{byte(db.DATA_Header)}, key...)
		if err := writeBlockToFile(item+"_"+keystr+".txt", path, prefix); err != nil {
			fmt.Println(err)
			return err
		}
		return nil
	default:
		return nil
	}

	if err := writeDBItermToFile(item+"_"+keystr+".txt", path, prefix); err != nil {
		fmt.Println(err)
		return err
	}

	return nil
}

func writeDBItermToFile(filename string, dbPath string, key []byte) error {
	st, err := db.NewLevelDBStore(dbPath)
	if err != nil {
		return err
	}
	if exist, err := PathExists("./exports"); err != nil {
		return err
	} else {
		if !exist {
			if err := os.Mkdir("./exports", os.ModePerm); err != nil {
				return err
			}
		}
	}
	f, err := os.Create("./exports/" + filename)
	if err != nil {
		return err
	}

	w := bufio.NewWriter(f)
	iter := st.NewIterator(key)
	for iter.Next() {
		w.WriteString(hex.EncodeToString(iter.Key()) + "," + hex.EncodeToString(iter.Value()) + "\n")
	}
	iter.Release()
	w.Flush()
	f.Close()
	st.Close()
	return nil
}

func PathExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

func writeBlockToFile(filename string, dbPath string, key []byte) error {
	st, err := db.NewLevelDBStore(dbPath)
	if err != nil {
		return err
	}
	if exist, err := PathExists("./exports"); err != nil {
		return err
	} else {
		if !exist {
			if err := os.Mkdir("./exports", os.ModePerm); err != nil {
				return err
			}
		}
	}
	f, err := os.Create("./exports/" + filename)
	if err != nil {
		return err
	}

	w := bufio.NewWriter(f)
	iter := st.NewIterator(key)
	for iter.Next() {
		b := new(ledger.Block)
		if err = b.FromTrimmedData(bytes.NewReader(iter.Value())); err != nil {
			return err
		}

		for i := 0; i < len(b.Transactions); i++ {
			hash := b.Transactions[i].Hash()
			value, err := st.Get(append([]byte{byte(db.DATA_Transaction)}, hash.ToArray()...))
			if err != nil {
				return err
			}

			txn := new(tx.Transaction)
			if err := txn.Deserialize(bytes.NewReader(value[4:])); err != nil {
				return err
			}

			b.Transactions[i] = txn
		}
		buff := bytes.NewBuffer(nil)
		b.Serialize(buff)
		w.WriteString(hex.EncodeToString(iter.Key()) + "," + hex.EncodeToString(buff.Bytes()) + "\n")
	}

	iter.Release()
	w.Flush()
	f.Close()
	st.Close()
	return nil
}
