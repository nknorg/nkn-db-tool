# nkn-db-tool

Export NKN's DB to files.

## INSTALL

1. clone the repository.

```
$ git clone https://github.com/nknorg/nkn-db-tool.git nknorg/nkn-db-tool
```

2. resolve dependencies.

```
$ git clone https://github.com/nknorg/nkn.git nknorg/nkn

$ glide install
```	

3. build.

```
$ go build dbexport.go
```

## USAGE

```
$ dbexport export [command options] [args]
```

OPTIONS:  
   --path value  the path of db  
   --item value  the prefix of db. include version, currentblockhash, bookkeeper,asset,issued,prepaid, unspent,utxo,transaction,header,blockhash,block  
   --key value   the key of item, hex string (optional)  

example

```
$ ./dbexport export --path ./Chain --item block --key bfffbe0c0be3aa7e9452180b03d0c82efc904acf2348d4fd4c2e4a915e70ae28
```
