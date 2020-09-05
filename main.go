package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

type File = *DBFile

type DB struct {
	*DBFile
	index map[string]int64
}

func (d DB) Init(filepath string) DB {
	d.DBFile = OpenDBFile(filepath)
	return d.Reindex()
}

func (d DB) Reindex() DB {
	d.MoveToStart()
	defer d.MoveToEnd()

	d.index = make(map[string]int64)

	buf, offset := bufio.NewReaderSize(d.DBFile, 4096), int64(0)
	for ln, err := buf.ReadString('\n'); err == nil; ln, err = buf.ReadString('\n') {
		key, _ := d.ParseEntry(ln)
		d.index[key] = offset
		offset += int64(len(ln))
	}

	return d
}

func (d DB) Write(key, value string) DB {
	offset := d.WriteEntry(key, value)
	d.index[key] = offset
	return d
}

func (d DB) Debug(key string) DB {
	if offset, found := d.index[key]; found {
		fmt.Printf("key: %s: offset = %d\n", key, offset)
		if fileSize := d.CurrentOffset(); offset > fileSize {
			fmt.Printf("offset exceeds file size of %d\n", fileSize)
			return d
		}
		fmt.Printf("entry at %d: %s\n", offset, d.ReadEntry(offset))
	} else {
		fmt.Printf("key: %s: not found in index\n", key)
	}
	return d
}

func (d DB) Read(key string, value *string) DB {
	if offset, found := d.index[key]; found {
		k, v := d.ReadParseEntry(offset)
		if k != key {
			panic(fmt.Errorf("index corrupt"))
		}

		*value = v
	}

	return d
}

func (d DB) Shutdown() {
	d.DBFile.Close()
}

func main() {
	var db DB
	db = DB.Init(db, "test.txt")
	defer db.Shutdown()
	displayInterface(db)
}

func displayInterface(db DB) {
	fmt.Print("> ")

	listen := bufio.NewScanner(os.Stdin)
	for listen.Scan() {

		cmdParts := strings.SplitN(listen.Text(), " ", 3)
		switch cmd := cmdParts[0]; cmd {
		case "quit":
			return
		case "write":
			if len(cmdParts) < 3 {
				fmt.Println("missing the key and/or value arguments; try 'write <key> <value>'.")
				break
			}
			k, v := cmdParts[1], cmdParts[2]
			db = db.Write(k, v)
			fmt.Println("written")
		case "read":
			if len(cmdParts) < 2 {
				fmt.Println("missing the key argument; try 'read <key>'.")
				break
			}
			k, v := cmdParts[1], ""
			db = db.Read(k, &v)
			fmt.Printf("%s = %s\n", k, v)
		case "debug":
			if len(cmdParts) < 2 {
				fmt.Println("missing the key argument; try 'debug <key>'.")
				break
			}
			k := cmdParts[1]
			db = db.Debug(k)
		case "reindex":
			db = db.Reindex()
		default:
			fmt.Println(`Command Help:
  quit 	              : Quits the application
  write <key> <value> : Writes the value to the key
  read <key>          : Returns the value for key
  reindex             : Rebuilds the database index`)
		}
		fmt.Print("> ")
	}
}
