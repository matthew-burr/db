package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/matthew-burr/db/database"
)

func main() {
	db := database.Init("test")
	defer db.Shutdown()
	displayInterface(db)
}

func displayInterface(db *database.DB) {
	fmt.Print("> ")

	listen := bufio.NewScanner(os.Stdin)
	for listen.Scan() {

		cmdParts := strings.SplitN(listen.Text(), " ", 3)
		switch cmd := cmdParts[0]; cmd {
		case "quit":
			fallthrough
		case "q":
			return
		case "write":
			fallthrough
		case "w":
			if len(cmdParts) < 3 {
				fmt.Println("missing the key and/or value arguments; try 'write <key> <value>'.")
				break
			}
			k, v := cmdParts[1], cmdParts[2]
			db.Write(k, v)
			fmt.Println("written")
		case "read":
			fallthrough
		case "r":
			if len(cmdParts) < 2 {
				fmt.Println("missing the key argument; try 'read <key>'.")
				break
			}
			entry := db.Read(cmdParts[1])
			fmt.Printf("%s: %s\n", entry.Key(), entry.Value())
		case "delete":
			fallthrough
		case "d":
			if len(cmdParts) < 2 {
				fmt.Println("missing the key argument; try 'delete <key>'.")
				break
			}
			db.Delete(cmdParts[1])
			fmt.Println("deleted")
		case "debug":
			if len(cmdParts) < 2 {
				fmt.Println("missing the key argument; try 'debug <key>'.")
				break
			}
			k := cmdParts[1]
			db.Debug(k)
		case "reindex":
			db.DBFile.File.Reindex()
		default:
			fmt.Println(`Command Help:
  q(uit)                : Quits the application
  w(rite) <key> <value> : Writes the value to the key
  r(ead) <key>          : Returns the value for key
  d(elete) <key>        : Deletes the key from the database
  reindex               : Rebuilds the database index`)
		}
		fmt.Print("> ")
	}
}
