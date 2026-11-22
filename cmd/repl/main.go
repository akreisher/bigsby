package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/akreisher/bigsby/internal/lsm"
)

const PROMPT = ">> "

func insert(db *lsm.LSMTree, out io.Writer, args ...string) error {
	if len(args) < 2 {
		return fmt.Errorf("Not enough arguments (expected 2).")
	}
	key := args[0]
	value := strings.Join(args[1:], " ")
	err := db.Insert(key, value)
	if err != nil {
		return err
	}
	io.WriteString(out, fmt.Sprintf("Inserted (%s, %s)\n", key, value))
	return nil
}

func search(db *lsm.LSMTree, out io.Writer, args ...string) error {
	if len(args) < 1 {
		return fmt.Errorf("Not enough arguments (expected 1).")
	}
	key := args[0]
	value, err := db.Search(key)
	if err != nil {
		return err
	}

	if value != nil {
		io.WriteString(out, *value+"\n")
	}
	return nil
}

func remove(db *lsm.LSMTree, out io.Writer, args ...string) error {
	if len(args) < 1 {
		return fmt.Errorf("Not enough arguments (expected 1).")
	}
	key := args[0]
	err := db.Remove(key)
	if err != nil {
		return err
	}
	io.WriteString(out, fmt.Sprintf("Removed key %s\n", key))
	return nil
}

func flush(db *lsm.LSMTree, out io.Writer) error {
	err := db.Flush()
	if err != nil {
		return err
	}
	io.WriteString(out, "Flushed memtable to disk.\n")
	return nil
}

func printObject(db *lsm.LSMTree, out io.Writer, args ...string) error {
	if len(args) < 1 {
		return fmt.Errorf("Not enough arguments (expected 1).")
	}

	obj := args[0]
	switch obj {
	case "memtable", "m":
		db.PrintMemtable(out)
	case "segment", "s":
		if len(args) > 1 {
			db.PrintSegment(out, args[1])
			return nil
		}
		db.PrintSegments(out)
	default:
		return fmt.Errorf("Don't know how to print %s.", obj)
	}
	return nil
}

func help(out io.Writer) {
	io.WriteString(out, "Commands:\n")
	io.WriteString(out, "  (i)nsert [key] [value]\n")
	io.WriteString(out, "  (s)earch [key]\n")
	io.WriteString(out, "  (r)emove [key]\n")
	io.WriteString(out, "  (p)rint (m)emtable|(s)egment [segment]\n")
	io.WriteString(out, "  (f)lush\n")
	io.WriteString(out, "  (h)elp\n")
	io.WriteString(out, "  (q)uit\n")
}

func handleCommand(db *lsm.LSMTree, out io.Writer, cmdLine string) bool {
	parts := strings.Split(cmdLine, " ")
	cmd, args := parts[0], parts[1:]
	var err error

	switch cmd {
	case "insert", "i":
		err = insert(db, out, args...)
	case "search", "s":
		err = search(db, out, args...)
	case "remove", "r":
		err = remove(db, out, args...)
	case "print", "p":
		err = printObject(db, out, args...)
	case "flush", "f":
		err = flush(db, out)
	case "help", "h":
		help(out)
	case "quit", "q":
		return false
	default:
		io.WriteString(out, fmt.Sprintf("Unknown command: %s\n", cmd))
	}

	if err != nil {
		io.WriteString(out, fmt.Sprintf("Error executing command %s: %v\n", cmd, err))
	}

	return true
}

func main() {

	dataDirPtr := flag.String("data-dir", "./.bigsby", "Directory to store data.")
	cmd := flag.String("cmd", "", "Command to execute instead of starting a REPL.")
	compactionLimitPtr := flag.Int("compaction-limit", 1000, "Limit (bytes) for compaction")
	flag.Parse()

	in := os.Stdin
	out := os.Stdout
	scanner := bufio.NewScanner(in)
	scanner.Err()

	db, err := lsm.New(&lsm.Settings{
		CompactionLimit: *compactionLimitPtr,
		DataDirectory:   *dataDirPtr,
	})
	if err != nil {
		panic(fmt.Sprintf("Could not create db: %v", err))
	}

	if *cmd != "" {
		handleCommand(db, out, *cmd)
		return
	}

	io.WriteString(out, "Running BigsbyDB\n")
	running := true
	for running {
		fmt.Printf(PROMPT)
		scanned := scanner.Scan()
		if !scanned {
			return
		}

		err = scanner.Err()
		if err != nil {
			io.WriteString(out, fmt.Sprintf("Scan err: %v\n", err))
		}

		line := scanner.Text()
		running = handleCommand(db, out, line)
	}
}
