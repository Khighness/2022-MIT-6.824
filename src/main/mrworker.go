package main

//
// start a worker process, which is implemented
// in ../mr/worker.go. typically there will be
// multiple worker processes, talking to one coordinator.
//
// go run mrworker.go wc.so
//
// Please do not change this file.
//

import "6.824/mr"
import "plugin"
import "os"
import "fmt"
import "log"

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintf(os.Stderr, "Usage: mrworker xxx.so\n")
		os.Exit(1)
	}

	mapf, reducef := loadPlugin(os.Args[1])

	mr.Worker(mapf, reducef)
}

//
// load the application StateMap and StateReduce functions
// from a plugin file, e.g. ../mrapps/wc.so
//
func loadPlugin(filename string) (func(string, string) []mr.KeyValue, func(string, []string) string) {
	p, err := plugin.Open(filename)
	if err != nil {
		log.Fatalf("cannot load plugin %v", filename)
	}
	xmapf, err := p.Lookup("StateMap")
	if err != nil {
		log.Fatalf("cannot find StateMap in %v", filename)
	}
	mapf := xmapf.(func(string, string) []mr.KeyValue)
	xreducef, err := p.Lookup("StateReduce")
	if err != nil {
		log.Fatalf("cannot find StateReduce in %v", filename)
	}
	reducef := xreducef.(func(string, []string) string)

	return mapf, reducef
}
