package main

import (
	"flag"
	"github.com/akreisher/bigsby/internal/lsm"
	"log"
	"net/http"
)

type Server struct {
	db *lsm.LSMTree
}

func (s Server) ServeDB(w http.ResponseWriter, r *http.Request) {

	r.ParseForm()
	key := r.Form.Get("key")
	if key == "" {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("No key provided"))
		return
	}

	var response string
	var err error

	switch r.Method {
	case "GET":
		value, err := s.db.Search(key)
		if err != nil {
			break
		}
		if value != nil {
			response = *value
		}
	case "POST":
		if !r.Form.Has("value") {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte("No value provided"))
			return
		}
		value := r.Form.Get("value")
		err = s.db.Insert(key, value)
		if err != nil {
			break
		}
		response = "ok"
	case "DELETE":
		err = s.db.Remove(key)
		if err != nil {
			break
		}
		response = "ok"
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
		w.Write([]byte("Unsupported method"))
		return
	}

	if err != nil {
		log.Printf("Error serving %s request: %s", r.Method, err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Write([]byte(response))
}

func main() {
	dataDirPtr := flag.String("data-dir", "./.bigsby", "Directory to store data.")
	compactionLimitPtr := flag.Int("compaction-limit", 1000, "Limit (bytes) for compaction")
	flag.Parse()

	db, err := lsm.New(&lsm.Settings{
		CompactionLimit: *compactionLimitPtr,
		DataDirectory:   *dataDirPtr,
	})

	if err != nil {
		panic(err)
	}

	server := Server{db: db}

	http.Handle("/", http.FileServer(http.Dir("./static")))
	http.HandleFunc("/db", server.ServeDB)
	http.ListenAndServe(":8080", nil)
}
