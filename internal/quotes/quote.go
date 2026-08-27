// Package quotes zawiera model notowania i jego walidację.
//
// Celowo nie wie nic o Kafce ani o HTTP: to jest ta część, którą chcemy
// testować bez podnoszenia czegokolwiek.
package quotes

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

// Quote to pojedyncze notowanie instrumentu, tak jak przychodzi z Kafki.
type Quote struct {
	Ticker string    `json:"ticker"`
	Price  string    `json:"price"`
	At     time.Time `json:"quote_ts"`
}

var (
	// ErrPusteNotowanie oznacza wiadomość, której nie da się sparsować.
	// Takie wiadomości NIE nadają się do ponowienia — lecą prosto do DLQ.
	ErrPusteNotowanie = errors.New("notowanie jest puste")
	ErrBrakTickera    = errors.New("brak tickera")
	ErrZlaCena        = errors.New("cena musi być liczbą większą od zera")
	ErrBrakCzasu      = errors.New("brak znacznika czasu")
)

// Parse czyta notowanie z surowej wiadomości Kafki.
func Parse(surowe []byte) (Quote, error) {
	if len(strings.TrimSpace(string(surowe))) == 0 {
		return Quote{}, ErrPusteNotowanie
	}

	var q Quote
	if err := json.Unmarshal(surowe, &q); err != nil {
		return Quote{}, fmt.Errorf("niepoprawny JSON: %w", err)
	}

	q.Ticker = NormalizujTicker(q.Ticker)
	if err := q.Waliduj(); err != nil {
		return Quote{}, err
	}
	return q, nil
}

// NormalizujTicker sprowadza ticker do jednej postaci.
// "  cdr  " i "CDR" to ten sam instrument — tak samo jak po stronie API.
func NormalizujTicker(t string) string {
	return strings.ToUpper(strings.TrimSpace(t))
}

// Waliduj sprawdza reguły, których API i tak dopilnuje — ale lepiej odrzucić
// śmieć tutaj niż wysłać zapytanie, które na pewno wróci z kodem 422.
func (q Quote) Waliduj() error {
	if q.Ticker == "" {
		return ErrBrakTickera
	}
	if !DodatniaLiczba(q.Price) {
		return fmt.Errorf("%w: %q", ErrZlaCena, q.Price)
	}
	if q.At.IsZero() {
		return ErrBrakCzasu
	}
	return nil
}

// Klucz jest używany jako klucz partycjonowania w Kafce.
//
// Notowania tego samego instrumentu MUSZĄ trafiać na tę samą partycję,
// inaczej dwa notowania CDR mogłyby być przetwarzane równolegle przez
// dwóch konsumentów i kolejność cen przestałaby cokolwiek znaczyć.
func (q Quote) Klucz() []byte {
	return []byte(q.Ticker)
}
