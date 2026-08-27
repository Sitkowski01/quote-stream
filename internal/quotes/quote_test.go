package quotes

import (
	"errors"
	"testing"
	"time"
)

func TestNormalizujTicker(t *testing.T) {
	przypadki := map[string]string{
		"cdr":     "CDR",
		"  cdr  ": "CDR",
		"CDR":     "CDR",
		"":        "",
	}

	for wejscie, oczekiwane := range przypadki {
		if got := NormalizujTicker(wejscie); got != oczekiwane {
			t.Errorf("NormalizujTicker(%q) = %q, chciałem %q", wejscie, got, oczekiwane)
		}
	}
}

func TestDodatniaLiczba(t *testing.T) {
	dobre := []string{"1", "0.01", "178.40", "  12.5  ", "15840", "0.000001"}
	zle := []string{"", "0", "-1", "-0.01", "abc", "1,5", "NaN", "1e5x", "  "}

	for _, s := range dobre {
		if !DodatniaLiczba(s) {
			t.Errorf("DodatniaLiczba(%q) = false, chciałem true", s)
		}
	}
	for _, s := range zle {
		if DodatniaLiczba(s) {
			t.Errorf("DodatniaLiczba(%q) = true, chciałem false", s)
		}
	}
}

func TestDodatniaLiczbaNieTraciPrecyzji(t *testing.T) {
	// float64 nie umie zapisać 0.1 dokładnie. Gdyby walidacja szła przez float,
	// bardzo duże i bardzo dokładne ceny zaczęłyby się rozjeżdżać.
	bardzoDokladna := "15840.123456789012345678"
	if !DodatniaLiczba(bardzoDokladna) {
		t.Errorf("DodatniaLiczba(%q) = false, chciałem true", bardzoDokladna)
	}
}

func TestParsePoprawneNotowanie(t *testing.T) {
	surowe := []byte(`{"ticker":"  cdr  ","price":"178.40","quote_ts":"2026-08-27T10:00:00Z"}`)

	q, err := Parse(surowe)
	if err != nil {
		t.Fatalf("nieoczekiwany błąd: %v", err)
	}
	if q.Ticker != "CDR" {
		t.Errorf("ticker = %q, chciałem CDR", q.Ticker)
	}
	if q.Price != "178.40" {
		t.Errorf("cena = %q, chciałem 178.40", q.Price)
	}
	if string(q.Klucz()) != "CDR" {
		t.Errorf("klucz = %q, chciałem CDR", q.Klucz())
	}
}

func TestParseOdrzuca(t *testing.T) {
	przypadki := []struct {
		nazwa  string
		surowe string
		blad   error
	}{
		{"pusta wiadomość", "", ErrPusteNotowanie},
		{"same białe znaki", "   ", ErrPusteNotowanie},
		{"brak tickera", `{"price":"1","quote_ts":"2026-08-27T10:00:00Z"}`, ErrBrakTickera},
		{"pusty ticker", `{"ticker":"  ","price":"1","quote_ts":"2026-08-27T10:00:00Z"}`, ErrBrakTickera},
		{"cena zero", `{"ticker":"CDR","price":"0","quote_ts":"2026-08-27T10:00:00Z"}`, ErrZlaCena},
		{"cena ujemna", `{"ticker":"CDR","price":"-5","quote_ts":"2026-08-27T10:00:00Z"}`, ErrZlaCena},
		{"cena nie liczba", `{"ticker":"CDR","price":"abc","quote_ts":"2026-08-27T10:00:00Z"}`, ErrZlaCena},
		{"brak czasu", `{"ticker":"CDR","price":"1"}`, ErrBrakCzasu},
	}

	for _, p := range przypadki {
		t.Run(p.nazwa, func(t *testing.T) {
			_, err := Parse([]byte(p.surowe))
			if err == nil {
				t.Fatal("chciałem błąd, dostałem nil")
			}
			if !errors.Is(err, p.blad) {
				t.Errorf("błąd = %v, chciałem %v", err, p.blad)
			}
		})
	}
}

func TestParseNiepoprawnyJSON(t *testing.T) {
	if _, err := Parse([]byte(`{to nie jest json`)); err == nil {
		t.Fatal("chciałem błąd dla niepoprawnego JSON-a")
	}
}

func TestKluczPartycjonowania(t *testing.T) {
	// Ten sam instrument musi dawać ten sam klucz niezależnie od zapisu —
	// inaczej notowania CDR rozjechałyby się po partycjach i straciły kolejność.
	a := Quote{Ticker: NormalizujTicker("cdr"), Price: "1", At: time.Now()}
	b := Quote{Ticker: NormalizujTicker("  CDR "), Price: "2", At: time.Now()}

	if string(a.Klucz()) != string(b.Klucz()) {
		t.Errorf("klucze się różnią: %q vs %q", a.Klucz(), b.Klucz())
	}
}
