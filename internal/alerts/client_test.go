package alerts

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Sitkowski01/quote-stream/internal/quotes"
)

func przykladoweNotowanie() quotes.Quote {
	return quotes.Quote{
		Ticker: "CDR",
		Price:  "178.40",
		At:     time.Date(2026, 8, 27, 10, 0, 0, 0, time.UTC),
	}
}

func serwer(t *testing.T, obsluga http.HandlerFunc) *httptest.Server {
	t.Helper()
	s := httptest.NewServer(obsluga)
	t.Cleanup(s.Close)
	return s
}

func TestWysylaPoprawneZapytanie(t *testing.T) {
	var (
		metoda, sciezka, klucz, typ string
		cialo                       map[string]any
	)

	s := serwer(t, func(w http.ResponseWriter, r *http.Request) {
		metoda, sciezka = r.Method, r.URL.Path
		klucz = r.Header.Get("X-API-Key")
		typ = r.Header.Get("Content-Type")
		_ = decodeJSON(r, &cialo)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ticker":"CDR","price":"178.40","evaluated":2,"triggered":[]}`))
	})

	wynik, err := Nowy(s.URL, "sekretny-klucz", time.Second).Wyslij(context.Background(), przykladoweNotowanie())
	if err != nil {
		t.Fatalf("nieoczekiwany błąd: %v", err)
	}

	if metoda != http.MethodPost || sciezka != "/v1/quotes" {
		t.Errorf("poszło %s %s, chciałem POST /v1/quotes", metoda, sciezka)
	}
	if klucz != "sekretny-klucz" {
		t.Errorf("X-API-Key = %q", klucz)
	}
	if typ != "application/json" {
		t.Errorf("Content-Type = %q", typ)
	}
	if cialo["ticker"] != "CDR" || cialo["price"] != "178.40" {
		t.Errorf("ciało zapytania = %v", cialo)
	}
	if wynik.Evaluated != 2 {
		t.Errorf("evaluated = %d, chciałem 2", wynik.Evaluated)
	}
}

func TestCenaIdzieJakoTekst(t *testing.T) {
	// Gdyby cena szła jako liczba JSON-a, 178.40 wróciłoby jako 178.4,
	// a przy większych wartościach doszłoby zaokrąglenie.
	var cialo map[string]any
	s := serwer(t, func(w http.ResponseWriter, r *http.Request) {
		_ = decodeJSON(r, &cialo)
		_, _ = w.Write([]byte(`{"ticker":"CDR","price":"1","evaluated":0,"triggered":[]}`))
	})

	q := przykladoweNotowanie()
	q.Price = "15840.123456"
	_, _ = Nowy(s.URL, "k", time.Second).Wyslij(context.Background(), q)

	if _, ok := cialo["price"].(string); !ok {
		t.Fatalf("cena wysłana jako %T, chciałem string", cialo["price"])
	}
	if cialo["price"] != "15840.123456" {
		t.Errorf("cena = %v", cialo["price"])
	}
}

func TestKlasyfikacjaBledow(t *testing.T) {
	przypadki := []struct {
		kod         int
		przejsciowy bool
	}{
		{http.StatusBadRequest, false},
		{http.StatusUnauthorized, false},
		{http.StatusUnprocessableEntity, false},
		{http.StatusNotFound, false},
		{http.StatusTooManyRequests, true},
		{http.StatusInternalServerError, true},
		{http.StatusBadGateway, true},
		{http.StatusServiceUnavailable, true},
	}

	for _, p := range przypadki {
		s := serwer(t, func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(p.kod)
			_, _ = w.Write([]byte(`{"detail":"nie"}`))
		})

		_, err := Nowy(s.URL, "k", time.Second).Wyslij(context.Background(), przykladoweNotowanie())
		if err == nil {
			t.Fatalf("HTTP %d: chciałem błąd", p.kod)
		}
		if got := CzyPonawiac(err); got != p.przejsciowy {
			t.Errorf("HTTP %d: CzyPonawiac = %v, chciałem %v (%v)", p.kod, got, p.przejsciowy, err)
		}
	}
}

func TestNiedostepneApiJestBledemPrzejsciowym(t *testing.T) {
	// Adres, pod którym nic nie stoi — to nie jest wina notowania.
	_, err := Nowy("http://127.0.0.1:1", "k", 200*time.Millisecond).
		Wyslij(context.Background(), przykladoweNotowanie())

	if err == nil {
		t.Fatal("chciałem błąd")
	}
	if !CzyPonawiac(err) {
		t.Errorf("błąd sieci uznany za trwały: %v", err)
	}
}

func TestOdpowiedzNieBedacaJsonemJestPrzejsciowa(t *testing.T) {
	// Zwykle to proxy albo load balancer wstawia stronę HTML — warto ponowić.
	s := serwer(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`<html>502</html>`))
	})

	_, err := Nowy(s.URL, "k", time.Second).Wyslij(context.Background(), przykladoweNotowanie())
	if err == nil || !CzyPonawiac(err) {
		t.Errorf("chciałem błąd przejściowy, dostałem %v", err)
	}
}
