package main

import (
	"bigsby/repl"
	"os"
)

func main() {
	repl.Start(os.Stdin, os.Stdout)
}
