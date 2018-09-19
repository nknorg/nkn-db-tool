package main

import (
	"os"

	"github.com/urfave/cli"
)

func main() {
	app := cli.NewApp()
	app.Name = "dbtool"
	app.Version = "0.0.1"
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:  "path, p",
			Usage: "the path of db",
			Value: "./Chain",
		},
	}
	app.Commands = []cli.Command{
		*NewExportCommand(),
		*NewRollbackCommand(),
	}
	app.Run(os.Args)
}
