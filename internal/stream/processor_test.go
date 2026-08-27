package stream

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/Sitkowski01/quote-stream/internal/alerts"
	"github.com/Sitkowski01/quote-stream/internal/quotes"
	"github.com/Sitkowski01/quote-stream/internal/retry"
)

// ── atrapy ──

type atrapaApi struct {
	wywolania int
	odpowiedz func(n int) (alerts.Wynik, error)
}

func (a *atrapaApi) Wyslij(context.Context, quotes.Quote) (alerts.Wynik, error) {
	a.wywolania++
	return a.odpowiedz(a.wywolania)
}

type atrapaDlq struct {
	wiadomosci []string
	powody     []string
	blad       error
}

func (d *atrapaDlq) DoDLQ(_ context.Context, surowe []byte, powod string) error {
	if d.blad != nil {
		return d.blad
	}
	d.wiadomosci = append(d.wiadomosci, string(surowe))
	d.powody = append(d.powody, powod)
	return nil
}

func cichyLog() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func szybkiRetry() retry.Ustawienia {
	return retry.Ustawienia{Prob: 3, Baza: time.Millisecond, Maks: 2 * time.Millisecond}
}

const poprawne = `{"ticker":"CDR","price":"178.40","quote_ts":"2026-08-27T10:00:00Z"}`

// ── testy ──

func TestPoprawneNotowanieJestZatwierdzane(t *testing.T) {
	api := &atrapaApi{odpowiedz: func(int) (alerts.Wynik, error) {
		return alerts.Wynik{Evaluated: 1}, nil
	}}
	dlq := &atrapaDlq{}
	p := NowyProcessor(api, dlq, szybkiRetry(), cichyLog())

	d, err := p.Obsluz(context.Background(), []byte(poprawne))

	if err != nil || d != Zatwierdz {
		t.Fatalf("decyzja = %v, błąd = %v", d, err)
	}
	if len(dlq.wiadomosci) != 0 {
		t.Errorf("poprawne notowanie trafiło do DLQ")
	}
	if p.Stat.Przetworzone.Load() != 1 {
		t.Errorf("licznik przetworzonych = %d", p.Stat.Przetworzone.Load())
	}
}

func TestZepsutaWiadomoscIdzieDoDLQBezPonawiania(t *testing.T) {
	// Trucizna nie może zatrzymać partycji — odkładamy ją i idziemy dalej.
	api := &atrapaApi{odpowiedz: func(int) (alerts.Wynik, error) {
		t.Fatal("API nie powinno zostać wywołane")
		return alerts.Wynik{}, nil
	}}
	dlq := &atrapaDlq{}
	p := NowyProcessor(api, dlq, szybkiRetry(), cichyLog())

	d, err := p.Obsluz(context.Background(), []byte(`{to nie jest json`))

	if err != nil || d != Zatwierdz {
		t.Fatalf("decyzja = %v, błąd = %v", d, err)
	}
	if len(dlq.wiadomosci) != 1 {
		t.Fatalf("w DLQ jest %d wiadomości, chciałem 1", len(dlq.wiadomosci))
	}
	if api.wywolania != 0 {
		t.Errorf("API wywołane %d razy dla śmiecia", api.wywolania)
	}
}

func TestBladPrzejsciowyJestPonawianyAzDoSkutku(t *testing.T) {
	api := &atrapaApi{odpowiedz: func(n int) (alerts.Wynik, error) {
		if n < 3 {
			return alerts.Wynik{}, &alerts.BladPrzejsciowy{Kod: 503, Opis: "chwilowo"}
		}
		return alerts.Wynik{Evaluated: 1}, nil
	}}
	p := NowyProcessor(api, &atrapaDlq{}, szybkiRetry(), cichyLog())

	d, err := p.Obsluz(context.Background(), []byte(poprawne))

	if err != nil || d != Zatwierdz {
		t.Fatalf("decyzja = %v, błąd = %v", d, err)
	}
	if api.wywolania != 3 {
		t.Errorf("wywołań API = %d, chciałem 3", api.wywolania)
	}
}

func TestApiNieodpowiadajaceNIEZatwierdzaOffsetu(t *testing.T) {
	// To jest najważniejsza decyzja w całym potoku.
	// Notowanie jest poprawne — winne jest API. Gdybyśmy odłożyli je do DLQ,
	// restart backendu kasowałby dane. Zostawiamy offset: broker dostarczy
	// wiadomość ponownie, a idempotencja po stronie API zabezpiecza powtórkę.
	api := &atrapaApi{odpowiedz: func(int) (alerts.Wynik, error) {
		return alerts.Wynik{}, &alerts.BladPrzejsciowy{Kod: 502, Opis: "leży"}
	}}
	dlq := &atrapaDlq{}
	p := NowyProcessor(api, dlq, szybkiRetry(), cichyLog())

	d, err := p.Obsluz(context.Background(), []byte(poprawne))

	if d != NieZatwierdzaj {
		t.Fatalf("decyzja = %v, chciałem NieZatwierdzaj", d)
	}
	if err == nil {
		t.Error("chciałem błąd")
	}
	if len(dlq.wiadomosci) != 0 {
		t.Error("poprawne notowanie trafiło do DLQ przy awarii API")
	}
	if p.Stat.Wstrzymane.Load() != 1 {
		t.Errorf("licznik wstrzymanych = %d", p.Stat.Wstrzymane.Load())
	}
}

func TestBladTrwalyIdzieDoDLQBezPonawiania(t *testing.T) {
	api := &atrapaApi{odpowiedz: func(int) (alerts.Wynik, error) {
		return alerts.Wynik{}, &alerts.BladTrwaly{Kod: 422, Opis: "zła cena"}
	}}
	dlq := &atrapaDlq{}
	p := NowyProcessor(api, dlq, szybkiRetry(), cichyLog())

	d, err := p.Obsluz(context.Background(), []byte(poprawne))

	if err != nil || d != Zatwierdz {
		t.Fatalf("decyzja = %v, błąd = %v", d, err)
	}
	if api.wywolania != 1 {
		t.Errorf("wywołań API = %d — błąd trwały był ponawiany", api.wywolania)
	}
	if len(dlq.wiadomosci) != 1 {
		t.Fatalf("w DLQ jest %d wiadomości", len(dlq.wiadomosci))
	}
}

func TestNieudanyZapisDoDLQNieZatwierdzaOffsetu(t *testing.T) {
	// Gdybyśmy zatwierdzili, wiadomość zniknęłaby bez śladu: ani przetworzona,
	// ani odłożona. Lepiej zablokować partycję i zobaczyć to w metrykach.
	api := &atrapaApi{odpowiedz: func(int) (alerts.Wynik, error) {
		return alerts.Wynik{}, &alerts.BladTrwaly{Kod: 400, Opis: "nie"}
	}}
	dlq := &atrapaDlq{blad: errors.New("DLQ też leży")}
	p := NowyProcessor(api, dlq, szybkiRetry(), cichyLog())

	d, err := p.Obsluz(context.Background(), []byte(poprawne))

	if d != NieZatwierdzaj {
		t.Fatalf("decyzja = %v, chciałem NieZatwierdzaj", d)
	}
	if err == nil {
		t.Error("chciałem błąd")
	}
}

func TestAnulowanyKontekstNieZatwierdza(t *testing.T) {
	ctx, anuluj := context.WithCancel(context.Background())
	anuluj()

	api := &atrapaApi{odpowiedz: func(int) (alerts.Wynik, error) {
		return alerts.Wynik{}, &alerts.BladPrzejsciowy{Opis: "nieważne"}
	}}
	p := NowyProcessor(api, &atrapaDlq{}, szybkiRetry(), cichyLog())

	d, _ := p.Obsluz(ctx, []byte(poprawne))
	if d != NieZatwierdzaj {
		t.Errorf("decyzja = %v, chciałem NieZatwierdzaj przy wyłączaniu", d)
	}
}

func TestLiczyUruchomioneAlerty(t *testing.T) {
	api := &atrapaApi{odpowiedz: func(int) (alerts.Wynik, error) {
		w := alerts.Wynik{Evaluated: 3}
		w.Triggered = make([]struct {
			ID        string `json:"id"`
			Ticker    string `json:"ticker"`
			Direction string `json:"direction"`
			Threshold string `json:"threshold"`
		}, 2)
		return w, nil
	}}
	p := NowyProcessor(api, &atrapaDlq{}, szybkiRetry(), cichyLog())

	if _, err := p.Obsluz(context.Background(), []byte(poprawne)); err != nil {
		t.Fatalf("nieoczekiwany błąd: %v", err)
	}
	if p.Stat.Uruchomione.Load() != 2 {
		t.Errorf("licznik uruchomionych = %d, chciałem 2", p.Stat.Uruchomione.Load())
	}
}

func TestPowodTrafiaDoDLQ(t *testing.T) {
	dlq := &atrapaDlq{}
	p := NowyProcessor(&atrapaApi{}, dlq, szybkiRetry(), cichyLog())

	if _, err := p.Obsluz(context.Background(), []byte(`{"ticker":"","price":"1","quote_ts":"2026-08-27T10:00:00Z"}`)); err != nil {
		t.Fatalf("nieoczekiwany błąd: %v", err)
	}
	if len(dlq.powody) != 1 || dlq.powody[0] == "" {
		t.Fatalf("powód nie trafił do DLQ: %v", dlq.powody)
	}
}
