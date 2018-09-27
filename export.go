package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"os"

	"github.com/nknorg/nkn/common"
	"github.com/nknorg/nkn/common/serialization"
	"github.com/nknorg/nkn/core/asset"
	"github.com/nknorg/nkn/core/ledger"
	tx "github.com/nknorg/nkn/core/transaction"
	"github.com/nknorg/nkn/db"
	"github.com/urfave/cli"
)

type current struct {
	Key   string      `json:"key"`
	Value interface{} `json:"value"`
}

func NewExportCommand() *cli.Command {
	return &cli.Command{
		Name:        "export",
		Usage:       "export db items",
		Description: "export db items",
		ArgsUsage:   "[args]",
		Flags: []cli.Flag{
			cli.BoolFlag{
				Name:  "raw, r",
				Usage: "raw data or readable",
			},
			cli.StringFlag{
				Name:  "item, i",
				Usage: "the prefix of db. include version, currentblockhash, asset, issued, prepaid, unspent,utxo,transaction,header,blockhash, headerlist, block",
			},

			cli.StringFlag{
				Name:  "key, k",
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

	path := c.GlobalString("path")
	item := c.String("item")
	keystr := c.String("key")
	israw := c.Bool("raw")
	key, _ := hex.DecodeString(keystr)

	st, err := db.NewLevelDBStore(path)
	if err != nil {
		return err
	}

	//TODO block ,trimedblock
	switch item {
	case "version":
		prefix := []byte{byte(db.CFG_Version)}
		err = exportVersion(item+"_"+keystr+".txt", st, prefix, israw)
	case "currentblockhash":
		prefix := []byte{byte(db.SYS_CurrentBlock)}
		err = exportCurrentBlock(item+"_"+keystr+".txt", st, prefix, israw)
	case "asset":
		prefix := append([]byte{byte(db.ST_Info)}, key...)
		err = exportAsset(item+"_"+keystr+".txt", st, prefix, israw)
	case "issued":
		prefix := append([]byte{byte(db.ST_QuantityIssued)}, key...)
		err = exportIssued(item+"_"+keystr+".txt", st, prefix, israw)
	case "prepaid":
		prefix := append([]byte{byte(db.ST_Prepaid)}, key...)
		err = exportPrepaid(item+"_"+keystr+".txt", st, prefix, israw)
	case "blockhash":
		prefix := append([]byte{byte(db.DATA_BlockHash)}, key...)
		err = exportBlockhash(item+"_"+keystr+".txt", st, prefix, israw)
	case "header":
		prefix := append([]byte{byte(db.DATA_Header)}, key...)
		err = exportHeader(item+"_"+keystr+".txt", st, prefix, israw)
	case "transaction":
		prefix := append([]byte{byte(db.DATA_Transaction)}, key...)
		err = exportTransaction(item+"_"+keystr+".txt", st, prefix, israw)
	case "unspent":
		prefix := append([]byte{byte(db.IX_Unspent)}, key...)
		err = exportUnspent(item+"_"+keystr+".txt", st, prefix, israw)
	case "utxo":
		prefix := append([]byte{byte(db.IX_Unspent_UTXO)}, key...)
		err = exportUTXO(item+"_"+keystr+".txt", st, prefix, israw)
	case "headerlist":
		prefix := append([]byte{byte(db.IX_HeaderHashList)}, key...)
		err = exportHeaderlist(item+"_"+keystr+".txt", st, prefix, israw)
	case "block":
		prefix := append([]byte{byte(db.DATA_Header)}, key...)
		err = exportBlock(item+"_"+keystr+".txt", st, prefix, israw)
	default:
		cli.ShowSubcommandHelp(c)
	}

	st.Close()
	return err
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

func createFile(name string) (*os.File, error) {
	if exist, err := PathExists("./exports"); err != nil {
		return nil, err
	} else {
		if !exist {
			if err := os.Mkdir("./exports", os.ModePerm); err != nil {
				return nil, err
			}
		}
	}

	f, err := os.Create("./exports/" + name)
	if err != nil {
		return nil, err
	}

	return f, nil
}

func exportVersion(filename string, st *db.LevelDBStore, key []byte, israw bool) error {
	type value struct {
		Version string `json:"version"`
	}

	f, err := createFile(filename)
	if err != nil {
		return err
	}

	w := bufio.NewWriter(f)
	iter := st.NewIterator(key)
	for iter.Next() {
		if israw {
			w.WriteString(hex.EncodeToString(iter.Key()) + "," + hex.EncodeToString(iter.Value()) + "\n")

		} else {
			data, _ := json.Marshal(current{Key: hex.EncodeToString(iter.Key()), Value: value{hex.EncodeToString(iter.Value())}})
			w.WriteString(string(data) + "\n")
		}
	}
	iter.Release()
	w.Flush()
	f.Close()
	return nil
}

func exportCurrentBlock(filename string, st *db.LevelDBStore, key []byte, israw bool) error {
	type value struct {
		Hash   string `json:"hash"`
		Height uint32 `json:"height"`
	}

	f, err := createFile(filename)
	if err != nil {
		return err
	}

	w := bufio.NewWriter(f)
	iter := st.NewIterator(key)
	for iter.Next() {
		if israw {
			w.WriteString(hex.EncodeToString(iter.Key()) + "," + hex.EncodeToString(iter.Value()) + "\n")

		} else {
			r := bytes.NewReader(iter.Value())
			var hash common.Uint256
			hash.Deserialize(r)
			height, _ := serialization.ReadUint32(r)
			data, _ := json.Marshal(current{Key: hex.EncodeToString(iter.Key()), Value: value{hash.ToHexString(), height}})
			w.WriteString(string(data) + "\n")
		}
	}
	iter.Release()
	w.Flush()
	f.Close()
	return nil
}

func exportAsset(filename string, st *db.LevelDBStore, key []byte, israw bool) error {
	type value struct {
		Ass asset.Asset `json:"asset"`
	}

	f, err := createFile(filename)
	if err != nil {
		return err
	}

	w := bufio.NewWriter(f)
	iter := st.NewIterator(key)
	for iter.Next() {
		if israw {
			w.WriteString(hex.EncodeToString(iter.Key()) + "," + hex.EncodeToString(iter.Value()) + "\n")

		} else {
			ass := new(asset.Asset)
			r := bytes.NewReader(iter.Value())
			ass.Deserialize(r)
			data, _ := json.Marshal(current{Key: hex.EncodeToString(iter.Key()), Value: value{*ass}})
			w.WriteString(string(data) + "\n")
		}
	}
	iter.Release()
	w.Flush()
	f.Close()
	return nil
}

func exportIssued(filename string, st *db.LevelDBStore, key []byte, israw bool) error {
	type value struct {
		Amount common.Fixed64 `json:"amount"`
	}

	f, err := createFile(filename)
	if err != nil {
		return err
	}

	w := bufio.NewWriter(f)
	iter := st.NewIterator(key)
	for iter.Next() {
		if israw {
			w.WriteString(hex.EncodeToString(iter.Key()) + "," + hex.EncodeToString(iter.Value()) + "\n")

		} else {
			var amount common.Fixed64
			r := bytes.NewReader(iter.Value())
			amount.Deserialize(r)
			data, _ := json.Marshal(current{Key: hex.EncodeToString(iter.Key()), Value: value{amount}})
			w.WriteString(string(data) + "\n")
		}
	}
	iter.Release()
	w.Flush()
	f.Close()
	return nil
}

func exportPrepaid(filename string, st *db.LevelDBStore, key []byte, israw bool) error {
	type value struct {
		Amount common.Fixed64 `json:"amount"`
		Rates  common.Fixed64 `json:"rates"`
	}

	f, err := createFile(filename)
	if err != nil {
		return err
	}

	w := bufio.NewWriter(f)
	iter := st.NewIterator(key)
	for iter.Next() {
		if israw {
			w.WriteString(hex.EncodeToString(iter.Key()) + "," + hex.EncodeToString(iter.Value()) + "\n")

		} else {
			var amount, rates common.Fixed64
			r := bytes.NewReader(iter.Value())
			amount.Deserialize(r)
			rates.Deserialize(r)
			data, _ := json.Marshal(current{Key: hex.EncodeToString(iter.Key()), Value: value{amount, rates}})
			w.WriteString(string(data) + "\n")
		}
	}
	iter.Release()
	w.Flush()
	f.Close()
	return nil
}

func exportBlockhash(filename string, st *db.LevelDBStore, key []byte, israw bool) error {
	type value struct {
		Hash   string `json:"hash"`
		Height uint32 `json:"height"`
	}

	f, err := createFile(filename)
	if err != nil {
		return err
	}

	w := bufio.NewWriter(f)
	iter := st.NewIterator(key)
	for iter.Next() {
		if israw {
			w.WriteString(hex.EncodeToString(iter.Key()) + "," + hex.EncodeToString(iter.Value()) + "\n")

		} else {
			height := binary.LittleEndian.Uint32(iter.Key()[1:])
			data, _ := json.Marshal(current{Key: hex.EncodeToString(iter.Key()), Value: value{hex.EncodeToString(iter.Value()), height}})
			w.WriteString(string(data) + "\n")
		}
	}
	iter.Release()
	w.Flush()
	f.Close()
	return nil
}

func exportHeader(filename string, st *db.LevelDBStore, key []byte, israw bool) error {
	type value struct {
		Header string `json:"header"`
	}

	f, err := createFile(filename)
	if err != nil {
		return err
	}

	w := bufio.NewWriter(f)
	iter := st.NewIterator(key)
	for iter.Next() {
		if israw {
			w.WriteString(hex.EncodeToString(iter.Key()) + "," + hex.EncodeToString(iter.Value()) + "\n")

		} else {
			var h = new(ledger.Header)
			r := bytes.NewReader(iter.Value())
			serialization.ReadUint64(r)
			h.Deserialize(r)
			headerMarshal, _ := h.MarshalJson()
			data, _ := json.Marshal(current{Key: hex.EncodeToString(iter.Key()), Value: value{string(headerMarshal)}})
			w.WriteString(string(data) + "\n")
		}
	}
	iter.Release()
	w.Flush()
	f.Close()
	return nil
}

func exportTransaction(filename string, st *db.LevelDBStore, key []byte, israw bool) error {
	type value struct {
		Height      uint32 `json:"height"`
		Transaction string `json:"transaction"`
	}

	f, err := createFile(filename)
	if err != nil {
		return err
	}

	w := bufio.NewWriter(f)
	iter := st.NewIterator(key)
	for iter.Next() {
		if israw {
			w.WriteString(hex.EncodeToString(iter.Key()) + "," + hex.EncodeToString(iter.Value()) + "\n")

		} else {
			var txn tx.Transaction
			r := bytes.NewReader(iter.Value())
			height, _ := serialization.ReadUint32(r)
			txn.Deserialize(r)

			txMarshal, _ := txn.MarshalJson()
			data, _ := json.Marshal(current{Key: hex.EncodeToString(iter.Key()), Value: value{height, string(txMarshal)}})
			w.WriteString(string(data) + "\n")
		}
	}
	iter.Release()
	w.Flush()
	f.Close()
	return nil
}

func exportUnspent(filename string, st *db.LevelDBStore, key []byte, israw bool) error {
	type value struct {
		Index string `json:"index"`
	}

	f, err := createFile(filename)
	if err != nil {
		return err
	}

	w := bufio.NewWriter(f)
	iter := st.NewIterator(key)
	for iter.Next() {
		if israw {
			w.WriteString(hex.EncodeToString(iter.Key()) + "," + hex.EncodeToString(iter.Value()) + "\n")

		} else {
			unspentArray, _ := common.GetUint16Array(iter.Value())
			unspentMarshal, _ := json.Marshal(unspentArray)
			data, _ := json.Marshal(current{Key: hex.EncodeToString(iter.Key()), Value: value{string(unspentMarshal)}})
			w.WriteString(string(data) + "\n")
		}
	}
	iter.Release()
	w.Flush()
	f.Close()
	return nil
}

func exportUTXO(filename string, st *db.LevelDBStore, key []byte, israw bool) error {
	type utxo struct {
		Txid  string         `json:"txid"`
		Index uint32         `json:"index"`
		Value common.Fixed64 `json:"value"`
	}
	type value struct {
		UTXO []utxo `json:"utxo"`
	}

	f, err := createFile(filename)
	if err != nil {
		return err
	}

	w := bufio.NewWriter(f)
	iter := st.NewIterator(key)
	for iter.Next() {
		if israw {
			w.WriteString(hex.EncodeToString(iter.Key()) + "," + hex.EncodeToString(iter.Value()) + "\n")

		} else {
			r := bytes.NewReader(iter.Value())
			listNum, _ := serialization.ReadVarUint(r, 0)

			unspents := make([]utxo, 0)
			for i := 0; i < int(listNum); i++ {
				uu := new(tx.UTXOUnspent)
				uu.Deserialize(r)
				u := utxo{
					Txid:  uu.Txid.ToHexString(),
					Index: uu.Index,
					Value: uu.Value,
				}
				unspents = append(unspents, u)
			}

			data, _ := json.Marshal(current{Key: hex.EncodeToString(iter.Key()), Value: value{unspents}})
			w.WriteString(string(data) + "\n")
		}
	}
	iter.Release()
	w.Flush()
	f.Close()
	return nil
}

func exportHeaderlist(filename string, st *db.LevelDBStore, key []byte, israw bool) error {
	type headerlist struct {
		Amount uint64   `json:"amount"`
		List   []string `json:"list"`
	}

	f, err := createFile(filename)
	if err != nil {
		return err
	}

	w := bufio.NewWriter(f)
	iter := st.NewIterator(key)
	for iter.Next() {
		if israw {
			w.WriteString(hex.EncodeToString(iter.Key()) + "," + hex.EncodeToString(iter.Value()) + "\n")

		} else {
			r := bytes.NewReader(iter.Value())
			amount, _ := serialization.ReadVarUint(r, 0)

			headerIndex := make([]string, 0)
			for i := 0; i < int(amount); i++ {
				var listHash common.Uint256
				listHash.Deserialize(r)
				str := listHash.ToHexString()
				headerIndex = append(headerIndex, str)
			}

			data, _ := json.Marshal(current{Key: hex.EncodeToString(iter.Key()), Value: headerlist{amount, headerIndex}})
			w.WriteString(string(data) + "\n")
		}
	}
	iter.Release()
	w.Flush()
	f.Close()
	return nil
}

func exportBlock(filename string, st *db.LevelDBStore, key []byte, israw bool) error {
	type value struct {
		Block string `json:"block"`
	}

	f, err := createFile(filename)
	if err != nil {
		return err
	}

	w := bufio.NewWriter(f)
	iter := st.NewIterator(key)
	for iter.Next() {
		b := new(ledger.Block)
		r := bytes.NewReader(iter.Value())
		serialization.ReadUint64(r)
		if err = b.FromTrimmedData(r); err != nil {
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

		if israw {
			buff := bytes.NewBuffer(nil)
			b.Serialize(buff)
			w.WriteString(hex.EncodeToString(iter.Key()) + "," + hex.EncodeToString(buff.Bytes()) + "\n")
		} else {
			blockMarshal, _ := b.MarshalJson()
			data, _ := json.Marshal(current{Key: hex.EncodeToString(iter.Key()), Value: value{string(blockMarshal)}})
			w.WriteString(string(data) + "\n")
		}

	}

	iter.Release()
	w.Flush()
	f.Close()
	return nil
}
