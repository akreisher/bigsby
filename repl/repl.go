package repl

import (
	"bigsby/lsm"
	"bufio"
	"fmt"
	"io"
	"strings"
)

const PROMPT = ">> "

func insert(db *lsm.LSMTree, out io.Writer, args ...string) {
	if len(args) < 2 {
		io.WriteString(out, "Not enough arguments (expected 2).\n")
		return
	}
	key := args[0]
	value := strings.Join(args[1:], " ")
	err := db.Insert(key, value)
	if err != nil {
		io.WriteString(out, fmt.Sprintf("Failed to insert: %s\n", err))
	}
	io.WriteString(out, fmt.Sprintf("Inserted [%s, %s]\n", key, value))
}

func search(db *lsm.LSMTree, out io.Writer, args ...string) {
	if len(args) < 1 {
		io.WriteString(out, "Not enough arguments (expected 1).\n")
		return
	}
	key := args[0]
	value, err := db.Search(key)
	if err != nil {
		io.WriteString(out, fmt.Sprintf("Failed to search: %s\n", err))
		return
	}

	if value == nil {
		io.WriteString(out, fmt.Sprintf("No value found for key %s\n", key))
	} else {
		io.WriteString(out, *value+"\n")
	}

}

func remove(db *lsm.LSMTree, out io.Writer, args ...string) {
	if len(args) < 1 {
		io.WriteString(out, "Not enough arguments (expected 1).\n")
		return
	}
	key := args[0]
	err := db.Remove(key)
	if err != nil {
		io.WriteString(out, fmt.Sprintf("Failed to remove: %s\n", err))
		return
	}
	io.WriteString(out, fmt.Sprintf("Removed key %s\n", key))
}

func flush(db *lsm.LSMTree, out io.Writer, args ...string) {
	db.Flush()
}

func print(db *lsm.LSMTree, out io.Writer, args ...string) {
	if len(args) < 1 {
		io.WriteString(out, "Not enough arguments (expected 1).\n")
		return
	}

	obj := args[0]
	switch obj {
	case "memtable", "m":
		db.PrintMemtable(out)
	case "segment", "s":
		db.PrintSegment(out)

	default:
		io.WriteString(out, fmt.Sprintf("Don't know how to print %s.\n", obj))
	}
}

func Start(in io.Reader, out io.Writer) {
	scanner := bufio.NewScanner(in)

	db, err := lsm.New(&lsm.Settings{
		CompactionLimit: 1000,
		DataDirectory:   "./",
	})

	if err != nil {
		panic("Could not create db")
	}

	io.WriteString(out, "Running BigsbyDB\n")
	running := true

	for running {
		fmt.Printf(PROMPT)
		scanned := scanner.Scan()
		if !scanned {
			return
		}

		line := scanner.Text()

		parts := strings.Split(line, " ")
		cmd, args := parts[0], parts[1:]

		switch cmd {
		case "insert", "i":
			insert(db, out, args...)
		case "search", "s":
			search(db, out, args...)
		case "remove", "r":
			remove(db, out, args...)
		case "print", "p":
			print(db, out, args...)
		case "flush", "f":
			flush(db, out, args...)
		case "quit", "q":
			running = false
		default:
			io.WriteString(out, fmt.Sprintf("Unknown command: %s\n", cmd))
		}
	}
}
