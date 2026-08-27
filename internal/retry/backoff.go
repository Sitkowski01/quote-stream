// Package retry to wykładniczy backoff z losowym rozrzutem.
package retry

import (
	"context"
	"math"
	"math/rand"
	"time"
)

// Ustawienia opisują, jak długo i jak gęsto ponawiamy.
type Ustawienia struct {
	Prob    int           // ile prób łącznie (pierwsza + ponowienia)
	Baza    time.Duration // odstęp po pierwszej nieudanej próbie
	Maks    time.Duration // górny limit pojedynczego odstępu
	Rozrzut float64       // 0.0–1.0, ułamek odstępu losowany w dół
}

// Domyslne to rozsądny punkt wyjścia dla konsumenta Kafki.
func Domyslne() Ustawienia {
	return Ustawienia{Prob: 4, Baza: 200 * time.Millisecond, Maks: 5 * time.Second, Rozrzut: 0.3}
}

// Odstep liczy przerwę przed próbą numer `proba` (licząc od 1).
//
// Rozrzut jest tu po to, żeby przy awarii API wszystkie repliki konsumenta
// nie wróciły dokładnie w tej samej milisekundzie i nie dobiły go ponownie.
func (u Ustawienia) Odstep(proba int) time.Duration {
	if proba < 1 {
		proba = 1
	}

	odstep := float64(u.Baza) * math.Pow(2, float64(proba-1))
	if odstep > float64(u.Maks) {
		odstep = float64(u.Maks)
	}

	if u.Rozrzut > 0 {
		odstep -= odstep * u.Rozrzut * rand.Float64()
	}
	return time.Duration(odstep)
}

// Ponawiaj wykonuje `akcja` aż do skutku albo do wyczerpania prób.
//
// `czyPonawiac` decyduje, czy dany błąd w ogóle nadaje się do ponowienia —
// bez tego serwis waliłby cztery razy w to samo zapytanie, które za każdym
// razem wróci z kodem 400.
func Ponawiaj(
	ctx context.Context,
	u Ustawienia,
	akcja func(ctx context.Context) error,
	czyPonawiac func(error) bool,
) error {
	var ostatni error

	for proba := 1; proba <= u.Prob; proba++ {
		if err := ctx.Err(); err != nil {
			return err
		}

		ostatni = akcja(ctx)
		if ostatni == nil {
			return nil
		}
		if !czyPonawiac(ostatni) {
			return ostatni
		}
		if proba == u.Prob {
			break
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(u.Odstep(proba)):
		}
	}

	return ostatni
}
