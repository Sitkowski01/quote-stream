package obs

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Sitkowski01/quote-stream/internal/stream"
)

func zapytaj(t *testing.T, s *Serwer, sciezka string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	s.http.Handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, sciezka, nil))
	return rec
}

func TestLivenessOdpowiadaZanimKonsumentWystartuje(t *testing.T) {
	// Gdyby /healthz czekał na Kafkę, wolny start wyglądałby jak awaria.
	s := Nowy(":0", &stream.Statystyki{})

	rec := zapytaj(t, s, "/healthz")

	if rec.Code != http.StatusOK {
		t.Errorf("kod = %d, chciałem 200", rec.Code)
	}
}

func TestReadinessCzekaNaKonsumenta(t *testing.T) {
	s := Nowy(":0", &stream.Statystyki{})

	if rec := zapytaj(t, s, "/readyz"); rec.Code != http.StatusServiceUnavailable {
		t.Errorf("przed startem kod = %d, chciałem 503", rec.Code)
	}

	s.Gotowy()

	if rec := zapytaj(t, s, "/readyz"); rec.Code != http.StatusOK {
		t.Errorf("po starcie kod = %d, chciałem 200", rec.Code)
	}
}

func TestMetrykiWFormaciePrometheusa(t *testing.T) {
	stat := &stream.Statystyki{}
	stat.Przetworzone.Add(7)
	stat.Odrzucone.Add(2)
	stat.Uruchomione.Add(3)

	tresc := zapytaj(t, Nowy(":0", stat), "/metrics").Body.String()

	for _, oczekiwane := range []string{
		"# TYPE quotes_processed_total counter",
		"quotes_processed_total 7",
		"quotes_rejected_total 2",
		"alerts_triggered_total 3",
	} {
		if !strings.Contains(tresc, oczekiwane) {
			t.Errorf("brak %q w metrykach:\n%s", oczekiwane, tresc)
		}
	}
}
