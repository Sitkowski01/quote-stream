package obs

import (
	"log/slog"
	"os"
	"strings"
)

// Logger pisze w JSON — w Kubernetesie i tak zbiera to agent,
// a tekst trzeba potem parsować.
func Logger(poziom string) *slog.Logger {
	var p slog.Level
	switch strings.ToLower(poziom) {
	case "debug":
		p = slog.LevelDebug
	case "warn":
		p = slog.LevelWarn
	case "error":
		p = slog.LevelError
	default:
		p = slog.LevelInfo
	}

	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: p}))
}
