// Package obs to sondy i metryki — to, czego Kubernetes potrzebuje,
// żeby wiedzieć, czy pod nadaje się do ruchu.
package obs

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/Sitkowski01/quote-stream/internal/stream"
)

// Serwer wystawia /healthz, /readyz i /metrics obok głównej pętli konsumenta.
type Serwer struct {
	http   *http.Server
	gotowy atomic.Bool
	stat   *stream.Statystyki
	start  time.Time
}

func Nowy(adres string, stat *stream.Statystyki) *Serwer {
	s := &Serwer{stat: stat, start: time.Now()}

	mux := http.NewServeMux()

	// Liveness: sam fakt, że proces odpowiada. Celowo nie sprawdza Kafki —
	// gdyby sprawdzał, chwilowa awaria brokera kazałaby Kubernetesowi
	// restartować całkiem zdrowe pody.
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"status":"ok"}`)
	})

	// Readiness: czy konsument faktycznie dołączył do grupy.
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if !s.gotowy.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			fmt.Fprint(w, `{"status":"startuje"}`)
			return
		}
		fmt.Fprint(w, `{"status":"ok"}`)
	})

	mux.HandleFunc("/metrics", s.metryki)

	s.http = &http.Server{
		Addr:              adres,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	return s
}

// Gotowy przełącza sondę readiness, gdy konsument wystartował.
func (s *Serwer) Gotowy() { s.gotowy.Store(true) }

func (s *Serwer) metryki(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")

	wiersz := func(nazwa, opis, typ string, wartosc int64) {
		fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s %s\n%s %d\n", nazwa, opis, nazwa, typ, nazwa, wartosc)
	}

	wiersz("quotes_processed_total", "Notowania przekazane do API", "counter", s.stat.Przetworzone.Load())
	wiersz("quotes_rejected_total", "Notowania odłożone do DLQ", "counter", s.stat.Odrzucone.Load())
	wiersz("quotes_held_total", "Wiadomości bez zatwierdzonego offsetu", "counter", s.stat.Wstrzymane.Load())
	wiersz("quotes_retries_total", "Ponowienia zapytań do API", "counter", s.stat.Ponowienia.Load())
	wiersz("alerts_triggered_total", "Alerty uruchomione przez notowania", "counter", s.stat.Uruchomione.Load())
	wiersz("consumer_uptime_seconds", "Czas życia procesu", "gauge", int64(time.Since(s.start).Seconds()))
}

func (s *Serwer) Start() {
	go func() {
		if err := s.http.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			panic(err)
		}
	}()
}

func (s *Serwer) Zamknij(ctx context.Context) error { return s.http.Shutdown(ctx) }
