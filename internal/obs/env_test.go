package obs

import (
	"reflect"
	"testing"
)

func TestEnvWracaDoDomyslnej(t *testing.T) {
	t.Setenv("QS_TEST", "")
	if got := Env("QS_TEST", "domyslna"); got != "domyslna" {
		t.Errorf("Env = %q", got)
	}

	t.Setenv("QS_TEST", "  wartosc  ")
	if got := Env("QS_TEST", "domyslna"); got != "wartosc" {
		t.Errorf("Env = %q — białe znaki nie zostały przycięte", got)
	}
}

func TestEnvListaRozdzielaIPomijaPuste(t *testing.T) {
	t.Setenv("QS_BROKERZY", "a:9092, b:9092 ,, c:9092")

	got := EnvLista("QS_BROKERZY", "")
	chce := []string{"a:9092", "b:9092", "c:9092"}

	if !reflect.DeepEqual(got, chce) {
		t.Errorf("EnvLista = %v, chciałem %v", got, chce)
	}
}

func TestEnvIntPrzyBzdurzeWracaDoDomyslnej(t *testing.T) {
	t.Setenv("QS_LICZBA", "nie-liczba")
	if got := EnvInt("QS_LICZBA", 42); got != 42 {
		t.Errorf("EnvInt = %d, chciałem 42", got)
	}

	t.Setenv("QS_LICZBA", "7")
	if got := EnvInt("QS_LICZBA", 42); got != 7 {
		t.Errorf("EnvInt = %d, chciałem 7", got)
	}
}
