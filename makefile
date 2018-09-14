.PHONY: all

all:
	go build -o dbtool dbtool.go export.go rollback.go
