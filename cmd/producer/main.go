// Producent: symuluje strumień notowań i publikuje je na temat wejściowy.
//
// W prawdziwym systemie w tym miejscu stoi feed dostawcy danych.
// Tu jest symulacja — błądzenie losowe z ciągnięciem do ceny otwarcia,
// takie samo jak w kliencie webowym, żeby oba końce mówiły to samo.
package main

import (
	"context"
	"encoding/json"
	"math/rand"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/Sitkowski01/quote-stream/internal/obs"
	"github.com/Sitkowski01/quote-stream/internal/quotes"
	"github.com/Sitkowski01/quote-stream/internal/stream"
)

type instrument struct {
	ticker    string
	cena      float64
	otwarcie  float64
	zmiennosc float64
}

func main() {
	log := obs.Logger(obs.Env("LOG_LEVEL", "info"))

	brokerzy := obs.EnvLista("KAFKA_BROKERS", "localhost:9092")
	temat := obs.Env("KAFKA_TOPIC", "quotes")
	odstep := time.Duration(obs.EnvInt("TICK_MS", 1000)) * time.Millisecond

	producent := stream.NowyProducent(brokerzy, temat)
	defer producent.Zamknij()

	rynek := []instrument{
		{"CDR", 178.40, 178.40, 0.006},
		{"PKN", 62.10, 62.10, 0.004},
		{"KGH", 148.70, 148.70, 0.007},
		{"ALE", 26.90, 26.90, 0.008},
		{"DNP", 396.50, 396.50, 0.005},
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	log.Info("producent wystartował", "temat", temat, "odstep", odstep, "instrumentow", len(rynek))

	tik := time.NewTicker(odstep)
	defer tik.Stop()

	wyslane := 0
	for {
		select {
		case <-ctx.Done():
			log.Info("producent zatrzymany", "wyslane", wyslane)
			return
		case <-tik.C:
			for i := range rynek {
				rynek[i].cena = nastepnaCena(rynek[i])

				q := quotes.Quote{
					Ticker: rynek[i].ticker,
					Price:  strconv.FormatFloat(rynek[i].cena, 'f', 2, 64),
					At:     time.Now().UTC(),
				}

				cialo, err := json.Marshal(q)
				if err != nil {
					log.Error("nie da się zserializować notowania", "blad", err)
					continue
				}

				if err := producent.Wyslij(ctx, q.Klucz(), cialo); err != nil {
					log.Error("nie udało się opublikować", "ticker", q.Ticker, "blad", err)
					continue
				}
				wyslane++
			}
		}
	}
}

func nastepnaCena(i instrument) float64 {
	szum := (rand.Float64() - 0.5) * 2 * i.zmiennosc
	powrot := (i.otwarcie - i.cena) / i.otwarcie * 0.05
	cena := i.cena * (1 + szum + powrot)
	if cena < 0.01 {
		cena = 0.01
	}
	return cena
}
