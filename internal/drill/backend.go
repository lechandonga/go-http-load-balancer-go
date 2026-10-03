// Package drill runs locally controllable mock upstreams for failure drills.
// Every instance is a plain net/http server whose behavior is changed at
// runtime through a small control API; only the standard library is used.
package drill

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"
)

// Backend is one controllable mock instance.
type Backend struct {
	Name string
	URL  string

	srv       *http.Server
	listener  net.Listener
	up        atomic.Bool
	fail5xx   atomic.Bool
	hang      atomic.Int64 // nanoseconds to hang every request
	requests  atomic.Int64
	completed atomic.Int64
	inflight  atomic.Int64
}

// Start launches an instance on 127.0.0.1:port (port 0 picks a free one).
func Start(name string, port int) (*Backend, error) {
	ln, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		return nil, err
	}
	b := &Backend{
		Name:     name,
		URL:      "http://" + ln.Addr().String(),
		listener: ln,
	}
	b.up.Store(true)

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", b.handleHealth)
	mux.HandleFunc("/control/", b.handleControl)
	mux.HandleFunc("/", b.handleApp)
	b.srv = &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = b.srv.Serve(ln) }()
	return b, nil
}

// Shutdown stops accepting new requests and lets in-flight handlers finish.
func (b *Backend) Shutdown(ctx context.Context) error { return b.srv.Shutdown(ctx) }

func (b *Backend) handleHealth(w http.ResponseWriter, r *http.Request) {
	if !b.up.Load() || b.fail5xx.Load() {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("down\n"))
		return
	}
	_, _ = w.Write([]byte("ok\n"))
}

// Control API (all POST, JSON responses):
//
//	/control/up            instance online (health + traffic)
//	/control/down          instance offline (/healthz -> 503)
//	/control/fail5xx?on=1  every response becomes 500 (?on=0 clears)
//	/control/hang?d=3s     every request hangs for d (?d=0 clears)
//	/control/state         current state and request counters
func (b *Backend) handleControl(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "post only", http.StatusMethodNotAllowed)
		return
	}
	q := r.URL.Query()
	switch r.URL.Path {
	case "/control/up":
		b.up.Store(true)
	case "/control/down":
		b.up.Store(false)
	case "/control/fail5xx":
		b.fail5xx.Store(q.Get("on") != "0")
	case "/control/hang":
		d := time.Duration(0)
		if v := q.Get("d"); v != "" {
			parsed, err := time.ParseDuration(v)
			if err != nil {
				http.Error(w, "bad duration", http.StatusBadRequest)
				return
			}
			d = parsed
		}
		b.hang.Store(int64(d))
	case "/control/state":
	default:
		http.Error(w, "unknown control action", http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = fmt.Fprintf(w,
		`{"name":%q,"up":%v,"fail5xx":%v,"hang":%q,"requests":%d,"completed":%d,"inflight":%d}`+"\n",
		b.Name, b.up.Load(), b.fail5xx.Load(),
		time.Duration(b.hang.Load()).String(), b.requests.Load(), b.completed.Load(), b.inflight.Load())
}

// InFlight reports requests currently being handled.
func (b *Backend) InFlight() int64 { return b.inflight.Load() }

func (b *Backend) handleApp(w http.ResponseWriter, r *http.Request) {
	b.requests.Add(1)
	b.inflight.Add(1)
	defer b.completed.Add(1)
	defer b.inflight.Add(-1)

	if d := time.Duration(b.hang.Load()); d > 0 {
		timer := time.NewTimer(d)
		select {
		case <-timer.C:
		case <-r.Context().Done():
			timer.Stop()
			return
		}
	}
	if !b.up.Load() {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("instance down\n"))
		return
	}
	if b.fail5xx.Load() {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("injected 500\n"))
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	_, _ = fmt.Fprintf(w, "hello from %s (path=%s, n=%s)\n",
		b.Name, r.URL.Path, strconv.FormatInt(b.requests.Load(), 10))
}
