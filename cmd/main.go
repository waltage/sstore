package main

import (
	"flag"
	"fmt"
	"os"

	"google.golang.org/grpc"

	"../../sstore"

	logz "github.com/waltage/dwio-logz"
)

var log = logz.DefaultLog("cmd")

func main() {

	var localPath string
	var dbName string
	var port int
	var address string
	var bucket string

	serveSet := flag.NewFlagSet("serve", flag.ExitOnError)
	cliBuckets := flag.NewFlagSet("buckets", flag.ExitOnError)
	cliKeys := flag.NewFlagSet("keys", flag.ExitOnError)

	serveSet.StringVar(&localPath, "dbpath", "./",
		"path to local filestore")
	serveSet.StringVar(&dbName, "dbname", "default",
		"name of local filestore <dbname>.db")
	serveSet.IntVar(&port, "port", 9001, "port")

	cliBuckets.StringVar(&address, "address", "localhost:9001",
		"server address")

	cliKeys.StringVar(&address, "address", "localhost:9001",
		"server address")
	cliKeys.StringVar(&bucket, "bucket", "DEFAULT",
		"bucket name")

	flag.Parse()
	flag.PrintDefaults()

	if flag.Arg(0) == "serve" {
		err := serveSet.Parse(flag.Args()[1:])
		if err != nil {
			flag.Usage()
			os.Exit(1)
		}
	} else if flag.Arg(0) == "buckets" {
		err := cliBuckets.Parse(flag.Args()[1:])
		if err != nil {
			flag.Usage()
			os.Exit(1)
		}
	} else if flag.Arg(0) == "keys" {
		err := cliKeys.Parse(flag.Args()[1:])
		if err != nil {
			flag.Usage()
			os.Exit(1)
		}
	} else {
		fmt.Println("missing a command: [serve | buckets | keys]")
		os.Exit(1)
	}

	if serveSet.Parsed() {

		srv := sstore.NewSStoreServer(localPath, dbName)
		srv.Serve(fmt.Sprintf("localhost:%d", port))

	} else if cliBuckets.Parsed() {
		clnt := sstore.SStoreClient{}
		clnt.Connect(address, grpc.WithInsecure())
		defer clnt.Close()

		fmt.Println("Connected to:", address)
		fmt.Println("Buckets:")
		buckets, err := clnt.ListBuckets()
		if err != nil {
			log.Error(err)
		}
		for _, b := range buckets {
			fmt.Println(" ", b)
		}
		fmt.Println("===========")
	} else if cliKeys.Parsed() {
		clnt := sstore.SStoreClient{}
		clnt.Connect(address, grpc.WithInsecure())
		defer clnt.Close()

		keys, err := clnt.ListKeys(bucket)
		if err != nil {
			log.Error(err)
		}
		for _, k := range keys {
			fmt.Println(k)
		}
	}

}
