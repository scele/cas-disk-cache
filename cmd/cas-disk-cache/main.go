package main

import (
	"fmt"
	"os"

	"github.com/jessevdk/go-flags"
	"github.com/scele/cas-disk-cache/pkg"
)

type args struct {
	DownloadWorkload string `short:"f" long:"file" description:"A file describing download workload" value-name:"FILE" required:"true"`
	Server           string `short:"s" long:"server" description:"Prefix where to download missing blobs" required:"true"`
	CachePath        string `short:"c" long:"cache-dir" description:"Local cache CAS directory" required:"true"`
	OutputDirectory  string `short:"o" long:"output-dir" description:"Output directory" default:"."`
	Concurrency  int `short:"j" long:"concurrency" description:"Download concurrency" default:"16"`
}

func main() {
	var args args
	var parser = flags.NewParser(&args, flags.Default|flags.IgnoreUnknown)

	_, err := parser.Parse()
	if err != nil {
		if flagsErr, ok := err.(*flags.Error); ok && flagsErr.Type == flags.ErrHelp {
			os.Exit(0)
		} else {
			os.Exit(1)
		}
	}
	dl := pkg.NewDownloader(args.CachePath, args.Concurrency)
	err = dl.Download(args.DownloadWorkload, args.Server, args.OutputDirectory)
	if err != nil {
		fmt.Printf("%v\n", err)
		os.Exit(1)
	}
}
