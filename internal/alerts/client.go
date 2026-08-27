// Package alerts to klient HTTP do price-alerts-api.
package alerts

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/Sitkowski01/quote-stream/internal/quotes"
)

// BladPrzejsciowy warto ponowić: API padło, przeciążyło się albo sieć mrugnęła.
type BladPrzejsciowy struct {
	Kod  int
	Opis string
}

func (e *BladPrzejsciowy) Error() string {
	if e.Kod == 0 {
		return fmt.Sprintf("błąd przejściowy: %s", e.Opis)
	}
	return fmt.Sprintf("błąd przejściowy (HTTP %d): %s", e.Kod, e.Opis)
}

// BladTrwaly nie ma sensu ponawiać — to samo zapytanie wróci tak samo.
// Takie notowanie leci do DLQ, a konsument idzie dalej.
type BladTrwaly struct {
	Kod  int
	Opis string
}

func (e *BladTrwaly) Error() string {
	return fmt.Sprintf("błąd trwały (HTTP %d): %s", e.Kod, e.Opis)
}

// CzyPonawiac to predykat dla pakietu retry.
func CzyPonawiac(err error) bool {
	var przejsciowy *BladPrzejsciowy
	return errors.As(err, &przejsciowy)
}

// Wynik odwzorowuje odpowiedź `POST /v1/quotes`.
type Wynik struct {
	Ticker    string `json:"ticker"`
	Price     string `json:"price"`
	Evaluated int    `json:"evaluated"`
	Triggered []struct {
		ID        string `json:"id"`
		Ticker    string `json:"ticker"`
		Direction string `json:"direction"`
		Threshold string `json:"threshold"`
	} `json:"triggered"`
}

// Client wysyła notowania do price-alerts-api.
type Client struct {
	baseURL string
	apiKey  string
	http    *http.Client
}

func Nowy(baseURL, apiKey string, timeout time.Duration) *Client {
	return &Client{
		baseURL: baseURL,
		apiKey:  apiKey,
		http:    &http.Client{Timeout: timeout},
	}
}

// Wyslij przekazuje notowanie do API.
//
// Ponowienie jest bezpieczne: API pilnuje unikalności pary
// (alert, znacznik czasu), więc powtórka nie zdubluje historii uruchomień.
// Dlatego cały potok może działać w trybie „co najmniej raz".
func (c *Client) Wyslij(ctx context.Context, q quotes.Quote) (Wynik, error) {
	cialo, err := json.Marshal(map[string]string{
		"ticker":   q.Ticker,
		"price":    q.Price,
		"quote_ts": q.At.UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		return Wynik{}, &BladTrwaly{Kod: 0, Opis: "nie da się zserializować notowania: " + err.Error()}
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/v1/quotes", bytes.NewReader(cialo))
	if err != nil {
		return Wynik{}, &BladTrwaly{Opis: err.Error()}
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-API-Key", c.apiKey)

	odp, err := c.http.Do(req)
	if err != nil {
		// Problem sieciowy albo timeout — API może za chwilę wrócić.
		return Wynik{}, &BladPrzejsciowy{Opis: err.Error()}
	}
	defer odp.Body.Close()

	tresc, _ := io.ReadAll(io.LimitReader(odp.Body, 8<<10))

	switch {
	case odp.StatusCode >= 200 && odp.StatusCode < 300:
		var w Wynik
		if err := json.Unmarshal(tresc, &w); err != nil {
			return Wynik{}, &BladPrzejsciowy{Kod: odp.StatusCode, Opis: "odpowiedź nie jest JSON-em"}
		}
		return w, nil

	case odp.StatusCode == http.StatusTooManyRequests, odp.StatusCode >= 500:
		// 429 i piątki znikają same — ponawiamy.
		return Wynik{}, &BladPrzejsciowy{Kod: odp.StatusCode, Opis: string(tresc)}

	default:
		// 400, 401, 422 — powtórzenie da ten sam wynik.
		return Wynik{}, &BladTrwaly{Kod: odp.StatusCode, Opis: string(tresc)}
	}
}
