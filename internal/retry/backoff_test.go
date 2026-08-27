package retry

import (
	"context"
	"errors"
	"testing"
	"time"
)

func bezRozrzutu() Ustawienia {
	return Ustawienia{Prob: 4, Baza: 100 * time.Millisecond, Maks: time.Second, Rozrzut: 0}
}

func TestOdstepRosnieWykladniczo(t *testing.T) {
	u := bezRozrzutu()
	oczekiwane := []time.Duration{100 * time.Millisecond, 200 * time.Millisecond, 400 * time.Millisecond}

	for i, chce := range oczekiwane {
		if got := u.Odstep(i + 1); got != chce {
			t.Errorf("Odstep(%d) = %v, chciałem %v", i+1, got, chce)
		}
	}
}

func TestOdstepNiePrzekraczaMaksimum(t *testing.T) {
	u := bezRozrzutu()
	// Bez limitu dziesiąta próba czekałaby ponad minutę.
	if got := u.Odstep(10); got != time.Second {
		t.Errorf("Odstep(10) = %v, chciałem %v", got, time.Second)
	}
}

func TestRozrzutTylkoSkracaOdstep(t *testing.T) {
	u := Ustawienia{Prob: 3, Baza: time.Second, Maks: time.Minute, Rozrzut: 0.3}

	for i := 0; i < 200; i++ {
		got := u.Odstep(1)
		if got > time.Second {
			t.Fatalf("rozrzut wydłużył odstęp do %v", got)
		}
		if got < 700*time.Millisecond {
			t.Fatalf("rozrzut skrócił odstęp za mocno: %v", got)
		}
	}
}

func TestPonawiajKonczyPoPierwszymSukcesie(t *testing.T) {
	wywolania := 0
	err := Ponawiaj(context.Background(), bezRozrzutu(),
		func(context.Context) error { wywolania++; return nil },
		func(error) bool { return true })

	if err != nil {
		t.Fatalf("nieoczekiwany błąd: %v", err)
	}
	if wywolania != 1 {
		t.Errorf("wywołań = %d, chciałem 1", wywolania)
	}
}

func TestPonawiajProbujeAzDoSkutku(t *testing.T) {
	wywolania := 0
	err := Ponawiaj(context.Background(), bezRozrzutu(),
		func(context.Context) error {
			wywolania++
			if wywolania < 3 {
				return errors.New("chwilowo nie działa")
			}
			return nil
		},
		func(error) bool { return true })

	if err != nil {
		t.Fatalf("nieoczekiwany błąd: %v", err)
	}
	if wywolania != 3 {
		t.Errorf("wywołań = %d, chciałem 3", wywolania)
	}
}

func TestPonawiajNieProbujeBleduTrwalego(t *testing.T) {
	// Zapytanie odrzucone jako niepoprawne wróci takie samo za każdym razem.
	// Ponawianie go to marnowanie czasu i dokładanie ruchu do padającego API.
	wywolania := 0
	trwaly := errors.New("400 nieprawidłowe dane")

	err := Ponawiaj(context.Background(), bezRozrzutu(),
		func(context.Context) error { wywolania++; return trwaly },
		func(err error) bool { return !errors.Is(err, trwaly) })

	if !errors.Is(err, trwaly) {
		t.Fatalf("błąd = %v, chciałem %v", err, trwaly)
	}
	if wywolania != 1 {
		t.Errorf("wywołań = %d, chciałem 1", wywolania)
	}
}

func TestPonawiajWyczerpujeProbyIZwracaOstatniBlad(t *testing.T) {
	wywolania := 0
	ostatni := errors.New("dalej nie działa")

	err := Ponawiaj(context.Background(), Ustawienia{Prob: 3, Baza: time.Millisecond, Maks: time.Millisecond},
		func(context.Context) error { wywolania++; return ostatni },
		func(error) bool { return true })

	if !errors.Is(err, ostatni) {
		t.Fatalf("błąd = %v, chciałem %v", err, ostatni)
	}
	if wywolania != 3 {
		t.Errorf("wywołań = %d, chciałem 3", wywolania)
	}
}

func TestPonawiajPrzerywaNaAnulowanymKontekscie(t *testing.T) {
	// Przy wyłączaniu serwisu nie chcemy czekać na dokończenie backoffu.
	ctx, anuluj := context.WithCancel(context.Background())
	wywolania := 0

	go func() { time.Sleep(20 * time.Millisecond); anuluj() }()

	err := Ponawiaj(ctx, Ustawienia{Prob: 10, Baza: 50 * time.Millisecond, Maks: time.Second},
		func(context.Context) error { wywolania++; return errors.New("nie działa") },
		func(error) bool { return true })

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("błąd = %v, chciałem context.Canceled", err)
	}
	if wywolania > 3 {
		t.Errorf("wywołań = %d — backoff nie zareagował na anulowanie", wywolania)
	}
}
