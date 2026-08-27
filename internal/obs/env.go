package obs

import (
	"os"
	"strconv"
	"strings"
)

// Env czyta zmienną środowiskową z wartością domyślną.
func Env(nazwa, domyslna string) string {
	if v := strings.TrimSpace(os.Getenv(nazwa)); v != "" {
		return v
	}
	return domyslna
}

// EnvLista rozdziela wartość po przecinkach — np. lista brokerów.
func EnvLista(nazwa, domyslna string) []string {
	surowe := strings.Split(Env(nazwa, domyslna), ",")
	var wynik []string
	for _, s := range surowe {
		if s = strings.TrimSpace(s); s != "" {
			wynik = append(wynik, s)
		}
	}
	return wynik
}

// EnvInt czyta liczbę; przy niepoprawnej wartości wraca do domyślnej.
func EnvInt(nazwa string, domyslna int) int {
	v, err := strconv.Atoi(Env(nazwa, ""))
	if err != nil {
		return domyslna
	}
	return v
}
