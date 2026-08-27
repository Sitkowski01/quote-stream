// Konsument: czyta notowania z Kafki i przekazuje je do price-alerts-api.
package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Sitkowski01/quote-stream/internal/alerts"
	"github.com/Sitkowski01/quote-stream/internal/obs"
	"github.com/Sitkowski01/quote-stream/internal/retry"
	"github.com/Sitkowski01/quote-stream/internal/stream"
)

func main() {
	log := obs.Logger(obs.Env("LOG_LEVEL", "info"))

	brokerzy := obs.EnvLista("KAFKA_BROKERS", "localhost:9092")
	temat := obs.Env("KAFKA_TOPIC", "quotes")
	tematDlq := obs.Env("KAFKA_DLQ_TOPIC", "quotes.dlq")
	grupa := obs.Env("KAFKA_GROUP", "quote-stream")
	apiURL := obs.Env("ALERTS_API_URL", "http://localhost:8000")
	apiKey := obs.Env("ALERTS_API_KEY", "")

	if apiKey == "" {
		log.Error("brak ALERTS_API_KEY — każdy zapis do API wróciłby z kodem 401")
		os.Exit(1)
	}

	klient := alerts.Nowy(apiURL, apiKey, 5*time.Second)
	dlq := stream.NowyDlq(brokerzy, tematDlq)
	defer dlq.Zamknij()

	ustRetry := retry.Ustawienia{
		Prob:    obs.EnvInt("RETRY_PROB", 4),
		Baza:    200 * time.Millisecond,
		Maks:    5 * time.Second,
		Rozrzut: 0.3,
	}

	proc := stream.NowyProcessor(klient, dlq, ustRetry, log)

	serwer := obs.Nowy(obs.Env("HTTP_ADDR", ":8080"), proc.Stat)
	serwer.Start()

	konsument := stream.NowyKonsument(
		stream.UstawieniaKonsumenta{Brokerzy: brokerzy, Temat: temat, Grupa: grupa},
		proc, log,
	)
	defer konsument.Zamknij()

	// Sygnał z systemu anuluje kontekst, a pętla dokańcza bieżącą wiadomość.
	// Bez tego SIGTERM przy wdrożeniu ucinałby przetwarzanie w połowie.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	serwer.Gotowy()
	log.Info("start", "brokerzy", brokerzy, "temat", temat, "grupa", grupa, "api", apiURL)

	if err := konsument.Pracuj(ctx); err != nil {
		log.Error("konsument zakończył się błędem", "blad", err)
	}

	zamykanie, anuluj := context.WithTimeout(context.Background(), 5*time.Second)
	defer anuluj()
	_ = serwer.Zamknij(zamykanie)

	log.Info("zatrzymany",
		"przetworzone", proc.Stat.Przetworzone.Load(),
		"odrzucone", proc.Stat.Odrzucone.Load(),
		"uruchomione_alerty", proc.Stat.Uruchomione.Load())
}
