package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/nknorg/nkn/common"
	"github.com/nknorg/nkn/common/serialization"
	"github.com/nknorg/nkn/core/ledger"
	tx "github.com/nknorg/nkn/core/transaction"
	"github.com/nknorg/nkn/core/transaction/payload"
	"github.com/nknorg/nkn/db"

	"github.com/urfave/cli"
)

func NewRollbackCommand() *cli.Command {
	return &cli.Command{
		Name:        "rollback",
		Usage:       "rollback db blocks",
		Description: "rollback db blocks",
		ArgsUsage:   "[args]",
		Flags: []cli.Flag{
			cli.StringFlag{
				Name:  "path, p",
				Usage: "the path of db",
			},

			cli.IntFlag{
				Name:  "num, n",
				Usage: "the number of blocks to be rollbacked",
				Value: 0,
			},
		},
		Action: rollbackAction,
		OnUsageError: func(c *cli.Context, err error, isSubcommand bool) error {
			return cli.NewExitError("", 1)
		},
	}
}

func rollbackAction(c *cli.Context) error {
	if c.NumFlags() < 2 {
		cli.ShowSubcommandHelp(c)
		return nil
	}

	path := c.String("path")
	num := c.Int("num")

	st, err := db.NewLevelDBStore(path)
	if err != nil {
		return err
	}

	for i := 0; i < num; i++ {
		if currentBlock, err := rollback(st); err != nil {
			fmt.Println("rollback err:", err)
			return err
		} else {
			hash := currentBlock.Hash()
			fmt.Printf("rollback block hash:%s, height:%d\n", hash.ToHexString(), currentBlock.Header.Height)
		}
	}

	st.Close()

	return nil
}

func rollback(st *db.LevelDBStore) (*ledger.Block, error) {
	if err := st.NewBatch(); err != nil {
		return nil, err
	}

	b, err := getCurrentBlock(st)
	if err != nil {
		return nil, err
	}
	if b.Header.Height == 0 {
		return nil, errors.New("this is the genesis block")
	}

	if err := rollbackHeader(st, b); err != nil {
		return nil, err
	}

	if err := rollbackTransaction(st, b); err != nil {
		return nil, err
	}

	if err := rollbackBlockHash(st, b); err != nil {
		return nil, err
	}

	if err := rollbackCurrentBlockHash(st, b); err != nil {
		return nil, err
	}

	if err := rollbackAsset(st, b); err != nil {
		return nil, err
	}

	if err := rollbackUnspentIndex(st, b); err != nil {
		return nil, err
	}

	if err := rollbackUTXO(st, b); err != nil {
		return nil, err
	}

	if err := rollbackPrepaidAndWithdraw(st, b); err != nil {
		return nil, err
	}

	if err := rollbackIssued(st, b); err != nil {
		return nil, err
	}

	if err := rollbackHeaderHashlist(st, b); err != nil {
		return nil, err
	}

	return b, st.BatchCommit()
}

func rollbackHeader(st *db.LevelDBStore, b *ledger.Block) error {
	blockHash := b.Hash()
	if err := st.BatchDelete(append([]byte{byte(db.DATA_Header)}, blockHash[:]...)); err != nil {
		return err
	}

	return nil
}

func rollbackTransaction(st *db.LevelDBStore, b *ledger.Block) error {
	for _, txn := range b.Transactions {
		txHash := txn.Hash()
		if err := st.BatchDelete(append([]byte{byte(db.DATA_Transaction)}, txHash[:]...)); err != nil {
			return err
		}
	}

	return nil
}

func rollbackBlockHash(st *db.LevelDBStore, b *ledger.Block) error {
	height := make([]byte, 4)
	binary.LittleEndian.PutUint32(height[:], b.Header.Height)
	return st.BatchDelete(append([]byte{byte(db.DATA_BlockHash)}, height...))
}

func rollbackCurrentBlockHash(st *db.LevelDBStore, b *ledger.Block) error {
	value := new(bytes.Buffer)
	if _, err := b.Header.PrevBlockHash.Serialize(value); err != nil {
		return err
	}
	if err := serialization.WriteUint32(value, b.Header.Height-1); err != nil {
		return err
	}

	return st.BatchPut([]byte{byte(db.SYS_CurrentBlock)}, value.Bytes())
}

func rollbackHeaderHashlist(st *db.LevelDBStore, b *ledger.Block) error {
	hash := b.Hash()
	iter := st.NewIterator([]byte{byte(db.IX_HeaderHashList)})
	var storedHeaderCount uint32
	var key []byte
	var headerIndex []common.Uint256
	for iter.Next() {
		key = iter.Key()
		headerIndex = make([]common.Uint256, 0)

		r := bytes.NewReader(iter.Value())
		storedHeaderCount, err := serialization.ReadVarUint(r, 0)
		if err != nil {
			return err
		}

		for i := 0; i < int(storedHeaderCount); i++ {
			var listHash common.Uint256
			listHash.Deserialize(r)
			headerIndex = append(headerIndex, listHash)
		}

		if hash.CompareTo(headerIndex[len(headerIndex)-1]) == 0 {
			headerIndex = headerIndex[:len(headerIndex)-1]
			storedHeaderCount--
			break
		}
	}
	iter.Release()

	var hashArray []byte
	for _, header := range headerIndex {
		hashArray = append(hashArray, header.ToArray()...)
	}
	hashBuffer := new(bytes.Buffer)
	serialization.WriteVarUint(hashBuffer, uint64(storedHeaderCount))
	hashBuffer.Write(hashArray)

	return st.BatchPut(key, hashBuffer.Bytes())
}

func rollbackUnspentIndex(st *db.LevelDBStore, b *ledger.Block) error {
	unspents := make(map[common.Uint256][]uint16)
	for _, txn := range b.Transactions {
		txhash := txn.Hash()
		st.BatchDelete(append([]byte{byte(db.IX_Unspent)}, txhash.ToArray()...))

		for _, input := range txn.Inputs {
			referTxnHash := input.ReferTxID
			referTxnOutIndex := input.ReferTxOutputIndex
			if _, ok := unspents[referTxnHash]; !ok {
				if unspentValue, err := st.Get(append([]byte{byte(db.IX_Unspent)}, referTxnHash.ToArray()...)); err != nil {
					return err
				} else {
					if unspents[referTxnHash], err = common.GetUint16Array(unspentValue); err != nil {
						return err
					}
				}
			}
			unspents[referTxnHash] = append(unspents[referTxnHash], referTxnOutIndex)
		}
	}

	for txhash, value := range unspents {
		st.BatchPut(append([]byte{byte(db.IX_Unspent)}, txhash.ToArray()...), common.ToByteArray(value))
	}

	return nil
}

func rollbackUTXO(st *db.LevelDBStore, b *ledger.Block) error {
	unspendUTXOs := make(map[common.Uint160]map[common.Uint256]map[uint32][]*tx.UTXOUnspent)
	height := b.Header.Height

	for _, txn := range b.Transactions {
		for _, output := range txn.Outputs {
			heightBuffer := make([]byte, 4)
			binary.LittleEndian.PutUint32(heightBuffer[:], height)
			key := append(append(output.ProgramHash.ToArray(), output.AssetID.ToArray()...), heightBuffer...)
			st.BatchDelete(append([]byte{byte(db.IX_Unspent_UTXO)}, key...))
		}

		for _, input := range txn.Inputs {
			referTxn, hh, err := getTransaction(st, input.ReferTxID)
			if err != nil {
				return err
			}

			index := input.ReferTxOutputIndex
			referTxnOutput := referTxn.Outputs[index]
			programHash := referTxnOutput.ProgramHash
			assetID := referTxnOutput.AssetID

			if _, ok := unspendUTXOs[programHash]; !ok {
				unspendUTXOs[programHash] = make(map[common.Uint256]map[uint32][]*tx.UTXOUnspent)
			}
			if _, ok := unspendUTXOs[programHash][assetID]; !ok {
				unspendUTXOs[programHash][assetID] = make(map[uint32][]*tx.UTXOUnspent)
			}
			if _, ok := unspendUTXOs[programHash][assetID][hh]; !ok {
				if unspendUTXOs[programHash][assetID][hh], err = getUTXOByHeight(st, programHash, assetID, hh); err != nil {
					unspendUTXOs[programHash][assetID][hh] = make([]*tx.UTXOUnspent, 0)
				}
			}

			u := tx.UTXOUnspent{
				Txid:  referTxn.Hash(),
				Index: uint32(index),
				Value: referTxnOutput.Value,
			}
			unspendUTXOs[programHash][assetID][hh] = append(unspendUTXOs[programHash][assetID][hh], &u)
		}

	}

	for programHash, programHash_value := range unspendUTXOs {
		for assetId, unspents := range programHash_value {
			for height, unspent := range unspents {
				heightBuffer := make([]byte, 4)
				binary.LittleEndian.PutUint32(heightBuffer[:], height)
				key := append(append(programHash.ToArray(), assetId.ToArray()...), heightBuffer...)

				listnum := len(unspent)
				if listnum == 0 {
					if err := st.BatchDelete(key); err != nil {
						return err
					}
					continue
				}

				w := bytes.NewBuffer(nil)
				serialization.WriteVarUint(w, uint64(listnum))
				for i := 0; i < listnum; i++ {
					unspent[i].Serialize(w)
				}

				if err := st.BatchPut(key, w.Bytes()); err != nil {
					return err
				}
			}

		}
	}

	return nil
}

func rollbackAsset(st *db.LevelDBStore, b *ledger.Block) error {
	for _, txn := range b.Transactions {
		if txn.TxType == tx.RegisterAsset {
			txhash := txn.Hash()
			if err := st.BatchDelete(append([]byte{byte(db.ST_Info)}, txhash.ToArray()...)); err != nil {
				return err
			}
		}
	}

	return nil
}

func rollbackIssued(st *db.LevelDBStore, b *ledger.Block) error {
	quantities := make(map[common.Uint256]common.Fixed64)

	for _, txn := range b.Transactions {
		if txn.TxType != tx.IssueAsset {
			continue
		}

		results := txn.GetMergedAssetIDValueFromOutputs()
		for assetId, value := range results {
			if _, ok := quantities[assetId]; !ok {
				quantities[assetId] += value
			} else {
				quantities[assetId] = value
			}
		}
	}

	for assetId, value := range quantities {
		data, err := st.Get(append([]byte{byte(db.ST_QuantityIssued)}, assetId.ToArray()...))
		if err != nil {
			return err
		}

		var qt common.Fixed64
		if err := qt.Deserialize(bytes.NewReader(data)); err != nil {
			return err
		}

		qt = qt - value
		quantity := bytes.NewBuffer(nil)
		if err := qt.Serialize(quantity); err != nil {
			return err
		}

		if err := st.BatchPut(append([]byte{byte(db.ST_QuantityIssued)}, assetId.ToArray()...), quantity.Bytes()); err != nil {
			return err
		}

	}

	return nil
}

func rollbackPrepaidAndWithdraw(st *db.LevelDBStore, b *ledger.Block) error {
	type prepaid struct {
		amount common.Fixed64
		rates  common.Fixed64
	}
	deposits := make(map[common.Uint160]prepaid)

	for _, txn := range b.Transactions {
		if txn.TxType != tx.Withdraw {
			continue
		}

		withdrawPld, ok := txn.Payload.(*payload.Withdraw)
		if !ok {
			return errors.New("transaction type error")
		}

		if _, ok := deposits[withdrawPld.ProgramHash]; !ok {
			if amount, rates, err := getPrepaid(st, withdrawPld.ProgramHash); err != nil {
				return err
			} else {
				deposits[withdrawPld.ProgramHash] = prepaid{amount: amount, rates: rates}
			}
		}

		newAmount := deposits[withdrawPld.ProgramHash].amount + txn.Outputs[0].Value
		deposits[withdrawPld.ProgramHash] = prepaid{amount: newAmount, rates: deposits[withdrawPld.ProgramHash].rates}
	}

	for _, txn := range b.Transactions {
		if txn.TxType != tx.Prepaid {
			continue
		}

		prepaidPld, ok := txn.Payload.(*payload.Prepaid)
		if !ok {
			return errors.New("this is not Prepaid transaciton")
		}

		pHash, err := txn.GetProgramHashes()
		if err != nil || len(pHash) == 0 {
			return errors.New("no programhash")
		}

		if _, ok := deposits[pHash[0]]; !ok {
			if amount, rates, err := getPrepaid(st, pHash[0]); err != nil {
				return err
			} else {
				deposits[pHash[0]] = prepaid{amount: amount, rates: rates}
			}
		}

		newAmount := deposits[pHash[0]].amount - prepaidPld.Amount
		deposits[pHash[0]] = prepaid{amount: newAmount, rates: deposits[pHash[0]].rates}
	}

	for programhash, deposit := range deposits {
		if deposit.amount == common.Fixed64(0) {
			st.BatchDelete(append([]byte{byte(db.ST_Prepaid)}, programhash.ToArray()...))
			continue
		}

		value := bytes.NewBuffer(nil)
		if err := deposit.amount.Serialize(value); err != nil {
			return err
		}

		if err := deposit.rates.Serialize(value); err != nil {
			return err
		}

		if err := st.BatchPut(append([]byte{byte(db.ST_Prepaid)}, programhash.ToArray()...), value.Bytes()); err != nil {
			return err
		}
	}

	return nil
}

func getUTXOByHeight(st *db.LevelDBStore, programHash common.Uint160, assetid common.Uint256, height uint32) ([]*tx.UTXOUnspent, error) {
	heightBuffer := make([]byte, 4)
	binary.LittleEndian.PutUint32(heightBuffer[:], height)
	key := append(append(programHash.ToArray(), assetid.ToArray()...), heightBuffer...)

	if unspentsData, err := st.Get(key); err != nil {
		return nil, err
	} else {
		r := bytes.NewReader(unspentsData)
		listNum, err := serialization.ReadVarUint(r, 0)
		if err != nil {
			return nil, err
		}

		unspents := make([]*tx.UTXOUnspent, listNum)
		for i := 0; i < int(listNum); i++ {
			uu := new(tx.UTXOUnspent)
			if err := uu.Deserialize(r); err != nil {
				return nil, err
			}

			unspents[i] = uu
		}

		return unspents, nil
	}
}

func getTransaction(st *db.LevelDBStore, hash common.Uint256) (*tx.Transaction, uint32, error) {
	value, err := st.Get(append([]byte{byte(db.DATA_Transaction)}, hash.ToArray()...))
	if err != nil {
		return nil, 0, err
	}

	r := bytes.NewReader(value)
	height, err := serialization.ReadUint32(r)
	if err != nil {
		return nil, 0, err
	}

	txn := new(tx.Transaction)
	if err := txn.Deserialize(r); err != nil {
		return nil, height, err
	}

	return txn, height, nil
}

func getPrepaid(st *db.LevelDBStore, programhash common.Uint160) (common.Fixed64, common.Fixed64, error) {
	value, err := st.Get(append([]byte{byte(db.ST_Prepaid)}, programhash.ToArray()...))
	if err != nil {
		return 0, 0, err
	}

	var amount, rates common.Fixed64
	r := bytes.NewReader(value)
	if err := amount.Deserialize(r); err != nil {
		return 0, 0, err
	}

	if err := rates.Deserialize(r); err != nil {
		return 0, 0, err
	}

	return amount, rates, nil
}

func getCurrentBlock(st *db.LevelDBStore) (*ledger.Block, error) {
	data, err := st.Get([]byte{byte(db.SYS_CurrentBlock)})
	if err != nil {
		return nil, err
	}

	var currentHash common.Uint256
	if err := currentHash.Deserialize(bytes.NewReader(data)); err != nil {
		return nil, err
	}

	header, err := st.Get(append([]byte{byte(db.DATA_Header)}, currentHash[:]...))
	if err != nil {
		return nil, err
	}

	b := new(ledger.Block)
	if err := b.FromTrimmedData(bytes.NewReader(header[8:])); err != nil {
		return nil, err
	}

	for i := 0; i < len(b.Transactions); i++ {
		if b.Transactions[i], _, err = getTransaction(st, b.Transactions[i].Hash()); err != nil {
			return nil, err
		}
	}

	return b, nil
}
