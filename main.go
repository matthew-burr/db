package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/matthew-burr/db/database"
)

func main() {
	db := database.Init("test.dat")
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
			db = db.Write(k, v)
			fmt.Println("written")
		case "read":
			fallthrough
		case "r":
			if len(cmdParts) < 2 {
				fmt.Println("missing the key argument; try 'read <key>'.")
				break
			}
			k, v := cmdParts[1], ""
			db = db.Read(k, &v)
			fmt.Printf("%s: %s\n", k, v)
		case "debug":
			if len(cmdParts) < 2 {
				fmt.Println("missing the key argument; try 'debug <key>'.")
				break
			}
			k := cmdParts[1]
			db = db.Debug(k)
		case "reindex":
			db.DBFile.Reindex()
		default:
			fmt.Println(`Command Help:
  q(uit)                : Quits the application
  w(rite) <key> <value> : Writes the value to the key
  r(ead) <key>          : Returns the value for key
  reindex               : Rebuilds the database index`)
		}
		fmt.Print("> ")
	}
}
