package repl

import (
	"bigsby/lsm"
	"bufio"
	"flag"
	"fmt"
	"io"
	"strings"
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
	io.WriteString(out, fmt.Sprintf("Inserted [%s, %s]\n", key, value))
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
		db.PrintSegment(out)

	default:
		return fmt.Errorf("Don't know how to print %s.", obj)
	}
	return nil
}

func Start(in io.Reader, out io.Writer) {

	dataDirPtr := flag.String("data-dir", "./.bigsby", "Directory to store data.")
	compactionLimitPtr := flag.Int("compaction-limit", 1000, "Limit (bytes) for compaction")
	flag.Parse()

	scanner := bufio.NewScanner(in)

	db, err := lsm.New(&lsm.Settings{
		CompactionLimit: *compactionLimitPtr,
		DataDirectory:   *dataDirPtr,
	})
	defer db.Flush()

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
		case "quit", "q":
			running = false
		default:
			io.WriteString(out, fmt.Sprintf("Unknown command: %s\n", cmd))
		}

		if err != nil {
			io.WriteString(out, fmt.Sprintf("Error executing command %s: %v\n", cmd, err))
		}
	}
}
