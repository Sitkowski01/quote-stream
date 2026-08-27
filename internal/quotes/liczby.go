package quotes

import (
	"math/big"
	"strings"
)

// DodatniaLiczba sprawdza cenę BEZ zamiany na float.
//
// Ceny chodzą przez system jako tekst od Kafki aż po kolumnę `numeric`
// w PostgreSQL — i tak ma zostać. Parsowanie do float64 po drodze
// wprowadziłoby błąd zaokrąglenia dokładnie tam, gdzie próg alertu
// jest porównaniem, a nie szacunkiem.
func DodatniaLiczba(s string) bool {
	s = strings.TrimSpace(s)
	if s == "" {
		return false
	}

	r, ok := new(big.Rat).SetString(s)
	if !ok {
		return false
	}
	return r.Sign() > 0
}
