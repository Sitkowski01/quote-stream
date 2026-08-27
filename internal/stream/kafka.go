package stream

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/segmentio/kafka-go"
)

// KafkaKonsument czyta notowania i oddaje je procesorowi.
//
// Offsety zatwierdzamy RĘCZNIE, dopiero po udanym przetworzeniu.
// Automatyczne zatwierdzanie potwierdzałoby wiadomości, których jeszcze
// nie wysłaliśmy do API — przy restarcie te notowania by przepadły.
type KafkaKonsument struct {
	reader *kafka.Reader
	proc   *Processor
	log    *slog.Logger
}

type UstawieniaKonsumenta struct {
	Brokerzy []string
	Temat    string
	Grupa    string
}

func NowyKonsument(u UstawieniaKonsumenta, proc *Processor, log *slog.Logger) *KafkaKonsument {
	return &KafkaKonsument{
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:  u.Brokerzy,
			Topic:    u.Temat,
			GroupID:  u.Grupa,
			MinBytes: 1,
			MaxBytes: 10 << 20,
			// Bez tego czytnik sam przesuwałby offsety w tle.
			CommitInterval: 0,
			MaxWait:        500 * time.Millisecond,
		}),
		proc: proc,
		log:  log,
	}
}

// Pracuj czyta w pętli aż do anulowania kontekstu.
func (k *KafkaKonsument) Pracuj(ctx context.Context) error {
	k.log.Info("konsument wystartował", "temat", k.reader.Config().Topic, "grupa", k.reader.Config().GroupID)

	for {
		wiad, err := k.reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				k.log.Info("konsument kończy pracę")
				return nil
			}
			return err
		}

		// Niezatwierdzenie offsetu NIE cofa czytnika: w obrębie tej samej sesji
		// kafka-go poszedłby po prostu do następnej wiadomości, a ta zostałaby
		// po cichu pominięta (wróciłaby dopiero po restarcie albo rebalansie).
		// Dlatego przy decyzji NieZatwierdzaj kręcimy się na TEJ SAMEJ wiadomości,
		// aż się uda — świadomie blokując postęp partycji zamiast zgubić notowanie.
		if !k.przetworzUparcie(ctx, wiad) {
			return nil
		}
	}
}

// przetworzUparcie powtarza przetwarzanie tej samej wiadomości aż do skutku.
// Zwraca false, gdy serwis się wyłącza.
func (k *KafkaKonsument) przetworzUparcie(ctx context.Context, wiad kafka.Message) bool {
	for proba := 1; ; proba++ {
		decyzja, blad := k.proc.Obsluz(ctx, wiad.Value)

		if decyzja == Zatwierdz {
			if err := k.reader.CommitMessages(ctx, wiad); err != nil {
				if errors.Is(err, context.Canceled) {
					return false
				}
				k.log.Error("nie udało się zatwierdzić offsetu", "blad", err)
			}
			return true
		}

		if ctx.Err() != nil {
			return false
		}

		k.log.Warn("wiadomość nieprzetworzona, ponawiam tę samą",
			"partycja", wiad.Partition, "offset", wiad.Offset, "proba", proba, "blad", blad)

		// Odstęp rośnie, ale nie ponad 10 s — przy dłuższej awarii nie ma sensu
		// dobijać padającej usługi, a partycja i tak stoi.
		przerwa := time.Duration(proba) * time.Second
		if przerwa > 10*time.Second {
			przerwa = 10 * time.Second
		}

		select {
		case <-ctx.Done():
			return false
		case <-time.After(przerwa):
		}
	}
}

func (k *KafkaKonsument) Zamknij() error { return k.reader.Close() }

// KafkaDlq odkłada wiadomości nie do przetworzenia na osobny temat.
type KafkaDlq struct {
	writer *kafka.Writer
}

func NowyDlq(brokerzy []string, temat string) *KafkaDlq {
	return &KafkaDlq{writer: &kafka.Writer{
		Addr:                   kafka.TCP(brokerzy...),
		Topic:                  temat,
		Balancer:               &kafka.LeastBytes{},
		AllowAutoTopicCreation: true,
		RequiredAcks:           kafka.RequireAll,
	}}
}

// DoDLQ zachowuje oryginalne bajty i dokłada powód w nagłówku.
// Oryginał zostaje nietknięty, żeby dało się go później odtworzyć.
func (d *KafkaDlq) DoDLQ(ctx context.Context, surowe []byte, powod string) error {
	return d.writer.WriteMessages(ctx, kafka.Message{
		Value: surowe,
		Headers: []kafka.Header{
			{Key: "powod", Value: []byte(powod)},
			{Key: "odlozone-o", Value: []byte(time.Now().UTC().Format(time.RFC3339))},
		},
	})
}

func (d *KafkaDlq) Zamknij() error { return d.writer.Close() }

// KafkaProducent publikuje notowania na temat wejściowy.
type KafkaProducent struct {
	writer *kafka.Writer
}

func NowyProducent(brokerzy []string, temat string) *KafkaProducent {
	return &KafkaProducent{writer: &kafka.Writer{
		Addr:  kafka.TCP(brokerzy...),
		Topic: temat,
		// Klucz = ticker, więc notowania jednego instrumentu trzymają się
		// jednej partycji i zachowują kolejność.
		Balancer:               &kafka.Hash{},
		AllowAutoTopicCreation: true,
		RequiredAcks:           kafka.RequireAll,
	}}
}

func (p *KafkaProducent) Wyslij(ctx context.Context, klucz, wartosc []byte) error {
	return p.writer.WriteMessages(ctx, kafka.Message{Key: klucz, Value: wartosc})
}

func (p *KafkaProducent) Zamknij() error { return p.writer.Close() }
