// Command demo-backend is a toy upstream for manually verifying the gateway.
//
//	go run ./cmd/demo-backend -addr :9001 -name a
//	curl 'localhost:9001/toggle'   # flip healthy/unhealthy
//	curl 'localhost:9001/?sleep=2' # slow response
package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"sync/atomic"
	"time"
)

func main() {
	addr := flag.String("addr", ":9001", "listen address")
	name := flag.String("name", "backend", "instance name echoed in responses")
	flag.Parse()

	var up atomic.Bool
	up.Store(true)

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		if !up.Load() {
			http.Error(w, "down", http.StatusServiceUnavailable)
			return
		}
		fmt.Fprintln(w, "ok")
	})
	mux.HandleFunc("/toggle", func(w http.ResponseWriter, r *http.Request) {
		up.Store(!up.Load())
		fmt.Fprintf(w, "%s up=%v\n", *name, up.Load())
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if s := r.URL.Query().Get("sleep"); s != "" {
			if sec, err := time.ParseDuration(s + "s"); err == nil {
				select {
				case <-time.After(sec):
				case <-r.Context().Done():
					return
				}
			}
		}
		fmt.Fprintf(w, "hello from %s (path=%s)\n", *name, r.URL.Path)
	})

	log.Printf("demo-backend %s listening on %s", *name, *addr)
	log.Fatal(http.ListenAndServe(*addr, mux))
}
