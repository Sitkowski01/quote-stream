// Package stream zawiera politykę przetwarzania wiadomości.
//
// Celowo nie zależy od Kafki: bierze surowe bajty i mówi, co z nimi zrobić.
// Dzięki temu najważniejsze decyzje potoku da się przetestować bez brokera.
package stream

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/Sitkowski01/quote-stream/internal/alerts"
	"github.com/Sitkowski01/quote-stream/internal/quotes"
	"github.com/Sitkowski01/quote-stream/internal/retry"
)

// Wysylacz to API alertów. Interfejs, bo w testach wchodzi atrapa.
type Wysylacz interface {
	Wyslij(ctx context.Context, q quotes.Quote) (alerts.Wynik, error)
}

// Nadawca odkłada wiadomość, której nie da się przetworzyć.
type Nadawca interface {
	DoDLQ(ctx context.Context, surowe []byte, powod string) error
}

// Decyzja mówi konsumentowi, co zrobić z offsetem.
type Decyzja int

const (
	// Zatwierdz — wiadomość jest załatwiona (przetworzona albo odłożona do DLQ).
	Zatwierdz Decyzja = iota
	// NieZatwierdzaj — problem jest po naszej stronie i minie.
	// Offset zostaje, broker dostarczy wiadomość ponownie.
	NieZatwierdzaj
)

type Processor struct {
	api  Wysylacz
	dlq  Nadawca
	ust  retry.Ustawienia
	log  *slog.Logger
	Stat *Statystyki
}

func NowyProcessor(api Wysylacz, dlq Nadawca, ust retry.Ustawienia, log *slog.Logger) *Processor {
	return &Processor{api: api, dlq: dlq, ust: ust, log: log, Stat: &Statystyki{}}
}

// Obsluz przetwarza jedną wiadomość i zwraca decyzję o offsecie.
func (p *Processor) Obsluz(ctx context.Context, surowe []byte) (Decyzja, error) {
	q, err := quotes.Parse(surowe)
	if err != nil {
		// Zepsutej wiadomości nie naprawi żadna liczba ponowień.
		// Odkładamy ją i idziemy dalej — jedna trucizna nie może zatrzymać partycji.
		p.log.Warn("wiadomość nie do sparsowania, idzie do DLQ", "blad", err)
		p.Stat.Odrzucone.Add(1)
		return p.doDLQ(ctx, surowe, "nie do sparsowania: "+err.Error())
	}

	var wynik alerts.Wynik
	err = retry.Ponawiaj(ctx, p.ust,
		func(ctx context.Context) error {
			var e error
			wynik, e = p.api.Wyslij(ctx, q)
			if e != nil {
				p.Stat.Ponowienia.Add(1)
			}
			return e
		},
		alerts.CzyPonawiac,
	)

	switch {
	case err == nil:
		p.Stat.Przetworzone.Add(1)
		if n := len(wynik.Triggered); n > 0 {
			p.Stat.Uruchomione.Add(int64(n))
			p.log.Info("notowanie uruchomiło alerty",
				"ticker", q.Ticker, "cena", q.Price, "alertow", n)
		}
		return Zatwierdz, nil

	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		// Serwis się wyłącza. Nie zatwierdzamy — dokończy to następny start.
		return NieZatwierdzaj, err

	case alerts.CzyPonawiac(err):
		// API leży mimo wszystkich prób. NIE odkładamy do DLQ i NIE zatwierdzamy:
		// notowanie jest w porządku, to backend ma problem. Broker dostarczy je
		// ponownie, a że potok jest idempotentny, powtórka nic nie zepsuje.
		p.Stat.Wstrzymane.Add(1)
		p.log.Error("API nie odpowiada po wyczerpaniu prób, offset zostaje",
			"ticker", q.Ticker, "blad", err)
		return NieZatwierdzaj, err

	default:
		// Błąd trwały: API mówi, że z tym notowaniem coś jest nie tak.
		p.log.Warn("API odrzuciło notowanie na stałe, idzie do DLQ",
			"ticker", q.Ticker, "blad", err)
		p.Stat.Odrzucone.Add(1)
		return p.doDLQ(ctx, surowe, "API odrzuciło: "+err.Error())
	}
}

func (p *Processor) doDLQ(ctx context.Context, surowe []byte, powod string) (Decyzja, error) {
	if err := p.dlq.DoDLQ(ctx, surowe, powod); err != nil {
		// Nie udało się nawet odłożyć na bok — wtedy lepiej nie zatwierdzać,
		// bo inaczej wiadomość zniknęłaby bez śladu.
		p.Stat.Wstrzymane.Add(1)
		return NieZatwierdzaj, fmt.Errorf("zapis do DLQ nie powiódł się: %w", err)
	}
	return Zatwierdz, nil
}
