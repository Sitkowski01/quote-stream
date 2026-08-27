package stream

import "sync/atomic"

// Statystyki są liczone atomowo, bo konsument może pracować w kilku goroutine'ach.
type Statystyki struct {
	Przetworzone atomic.Int64
	Odrzucone    atomic.Int64
	Wstrzymane   atomic.Int64
	Ponowienia   atomic.Int64
	Uruchomione  atomic.Int64
}
