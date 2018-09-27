# nkn-db-tool

Export NKN's DB to files or rollback it.

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
$ make
```

## USAGE

```
$ ./dbtool [global options] command [command options] [arguments...]

COMMANDS:
     export    export db items
     rollback  rollback db blocks
     help, h   Shows a list of commands or help for one command
```


OPTIONS:  
export command:  
   --raw, -r               raw data or readable  
   --item value, -i value  the prefix of db. include version, currentblockhash, asset, issued, prepaid, unspent,utxo,transaction,header,blockhash, headerlist,block 
   --key value, -k value   the key of item, hex string  

rollback command:  
 --num value, -n value  the number of blocks to be rollbacked (default: 0)  

example

```
$ ./dbexport --path ./Chain export --item header --key bfffbe0c0be3aa7e9452180b03d0c82efc904acf2348d4fd4c2e4a915e70ae28
```
