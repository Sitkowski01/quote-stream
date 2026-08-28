# quote-stream

[![CI](https://github.com/Sitkowski01/quote-stream/actions/workflows/ci.yml/badge.svg)](https://github.com/Sitkowski01/quote-stream/actions/workflows/ci.yml)

Potok zdarzeń w **Go**: konsument czyta notowania giełdowe z **Kafki** i przekazuje je
do [**price-alerts-api**](https://github.com/Sitkowski01/price-alerts-api), które ocenia
je względem progów i uruchamia alerty.

To brakujące ogniwo tamtego systemu. Wcześniej notowania trzeba było wpisywać ręcznie —
teraz przychodzą strumieniem, tak jak w rzeczywistości przychodziłyby od dostawcy danych.

```
producent ──► temat "quotes" ──► konsument (Go) ──► price-alerts-api ──► PostgreSQL
(symulacja)     3 partycje          │
                                    └──► temat "quotes.dlq"
```

## Co tu jest naprawdę do obejrzenia

Domena jest prosta — cała robota siedzi w tym, co się dzieje, **gdy coś pójdzie nie tak**.

### Trzy różne rodzaje porażki, trzy różne reakcje

| Sytuacja | Reakcja | Dlaczego |
|---|---|---|
| Wiadomość nie do sparsowania | DLQ, offset zatwierdzony | Żadna liczba ponowień nie naprawi zepsutego JSON-a. Jedna trucizna nie może zatrzymać partycji. |
| API odrzuca notowanie na stałe (400, 422) | DLQ, offset zatwierdzony | To samo zapytanie wróci tak samo. Ponawianie tylko dokłada ruchu. |
| API nie odpowiada (5xx, timeout, sieć) | **offset NIE zatwierdzony**, ponawianie tej samej wiadomości | Notowanie jest w porządku — to backend ma problem. Odłożenie go do DLQ znaczyłoby, że restart API kasuje dane. |

Ten podział jest w `internal/alerts/client.go` (klasyfikacja błędu) i
`internal/stream/processor.go` (decyzja o offsecie), a każdy wiersz tabeli ma swój test.

### Pułapka, w którą wpadłem i którą warto znać

Pierwsza wersja konsumenta przy błędzie przejściowym **nie zatwierdzała offsetu i szła
do następnej wiadomości** — z komentarzem, że broker dostarczy ją ponownie.

To nieprawda. Niezatwierdzenie offsetu **nie cofa czytnika**: w obrębie tej samej sesji
konsumenta wiadomość zostaje po prostu pominięta i wróci dopiero po restarcie albo
rebalansie. W praktyce oznaczało to cichą utratę notowania.

Poprawka jest w `przetworzUparcie`: przy decyzji „nie zatwierdzaj" konsument kręci się
na **tej samej** wiadomości z rosnącym odstępem, aż się uda. Partycja świadomie stoi —
lepiej zatrzymać strumień niż zgubić dane.

Znalazłem to dopiero uruchamiając cały stos i wrzucając zepsutą wiadomość do Kafki.
Testy jednostkowe tego nie widziały, bo sprawdzały decyzję procesora, a nie to,
co konsument z tą decyzją robi.

### Decyzje projektowe

- **Offsety zatwierdzane ręcznie**, dopiero po udanym przetworzeniu. Automatyczne
  potwierdzałoby wiadomości, których jeszcze nie wysłaliśmy do API — restart gubiłby dane.
- **Klucz partycjonowania to ticker.** Notowania jednego instrumentu trzymają się jednej
  partycji, więc zachowują kolejność. Bez tego dwa notowania CDR mogłyby być
  przetwarzane równolegle i kolejność cen przestałaby cokolwiek znaczyć.
- **Potok działa w trybie „co najmniej raz"** i jest to bezpieczne, bo API pilnuje
  unikalności pary `(alert, znacznik czasu)`. Powtórka nie zdubluje historii uruchomień.
- **Ceny nie są nigdy zamieniane na `float`.** Idą jako tekst od Kafki aż po kolumnę
  `numeric` w PostgreSQL. Walidacja używa `big.Rat` — zaokrąglenie binarne nie ma prawa
  wejść tam, gdzie próg alertu jest porównaniem, a nie szacunkiem.
- **Backoff ma losowy rozrzut.** Przy awarii API wszystkie repliki konsumenta nie wracają
  w tej samej milisekundzie i nie dobijają go ponownie.
- **Wyłączanie jest kontrolowane.** SIGTERM anuluje kontekst, bieżąca wiadomość zostaje
  dokończona. Bez tego wdrożenie ucinałoby przetwarzanie w połowie.
- **Liveness nie sprawdza Kafki**, readiness sprawdza. Gdyby `/healthz` zależał od brokera,
  chwilowa awaria Kafki kazałaby Kubernetesowi restartować zdrowe pody.

## Uruchomienie

Cały potok jednym poleceniem — Kafka w trybie KRaft, bez ZooKeepera:

```bash
# Obraz API zbuduj w sąsiednim repozytorium
docker build -t price-alerts-api:0.1.0 ../price-alerts-api

cd deploy
docker compose up -d
```

Wstaje sześć usług: Kafka, jednorazowe zadanie tworzące tematy, PostgreSQL,
price-alerts-api, konsument i producent. Konsument wystawia sondy i metryki
na porcie `8080`, API na `8000`.

```bash
# Alert, który rynek przebije od razu
curl -X POST localhost:8000/v1/alerts \
  -H "X-API-Key: klucz-lokalny" -H 'Content-Type: application/json' \
  -d '{"ticker":"PKN","direction":"below","threshold":"100.00"}'

# Po kilku sekundach
curl "localhost:8000/v1/alerts?ticker=PKN"     # status: triggered
curl localhost:8080/metrics
```

### Zmienne środowiskowe

| Zmienna | Domyślnie | Opis |
|---|---|---|
| `KAFKA_BROKERS` | `localhost:9092` | lista brokerów po przecinku |
| `KAFKA_TOPIC` | `quotes` | temat wejściowy |
| `KAFKA_DLQ_TOPIC` | `quotes.dlq` | temat na wiadomości nie do przetworzenia |
| `KAFKA_GROUP` | `quote-stream` | grupa konsumencka |
| `ALERTS_API_URL` | `http://localhost:8000` | adres price-alerts-api |
| `ALERTS_API_KEY` | — | **wymagana**, inaczej każdy zapis wróci z 401 |
| `RETRY_ATTEMPTS` | `4` | liczba prób przy błędzie przejściowym (pierwsza + ponowienia) |
| `TICK_MS` | `1000` | odstęp między notowaniami (producent) |

## Wynik uruchomienia

Poniżej faktyczny stan po podniesieniu stosu, założeniu jednego alertu
i wrzuceniu do Kafki jednej celowo zepsutej wiadomości.

```
$ curl -s localhost:8080/metrics
quotes_processed_total 36
quotes_rejected_total 1
quotes_held_total 0
quotes_retries_total 0
alerts_triggered_total 1

$ kafka-get-offsets.sh --topic quotes.dlq
quotes.dlq:0:1

$ curl -s "localhost:8000/v1/alerts?ticker=PKN"
{"total":1, "items":[{"ticker":"PKN","direction":"below","threshold":"100.000000",
                      "status":"triggered", ...}]}
```

Alert przeszedł w `triggered` **sam**, bez żadnego ręcznego zapytania — dojechało do
niego notowanie ze strumienia. Zepsuta wiadomość wylądowała w DLQ, a potok jechał dalej:
`quotes_held_total` zostało na zerze, więc nic nie zablokowało partycji.

Wiadomość w DLQ zachowuje oryginalne bajty, a powód idzie w nagłówkach:

```
powod: nie do sparsowania: niepoprawny JSON: invalid character 'z' looking for
       beginning of object key string
odlozone-o: 2026-08-27T19:33:05Z
--
{zepsuty json
```

Dzięki temu da się później odtworzyć, co dokładnie przyszło i dlaczego odpadło.

## Testy

```bash
go test ./...
go test -race ./...   # detektor wyścigów
```

**43 testy w siedmiu plikach**, przy dwunastu plikach kodu. Wszystkie przechodzą
z `-race` — statystyki konsumenta są liczone atomowo z kilku goroutine'ów,
więc to jest tu realna kontrola, nie ozdobnik.

| Pakiet | Co sprawdza |
|---|---|
| `internal/quotes` | parsowanie i walidacja notowania, normalizacja tickera, **walidacja ceny bez `float`** (w tym liczba z osiemnastoma cyframi po przecinku), stabilność klucza partycjonowania |
| `internal/retry` | wykładniczy wzrost odstępu, górny limit, rozrzut wyłącznie skracający, brak ponawiania błędu trwałego, przerwanie na anulowanym kontekście |
| `internal/alerts` | klasyfikacja ośmiu kodów HTTP na przejściowe i trwałe, niedostępne API jako błąd przejściowy, cena wysyłana jako tekst, odpowiedź nie będąca JSON-em |
| `internal/stream` | **cała tabela decyzji o offsecie**: śmieć do DLQ bez ponawiania, błąd przejściowy ponawiany do skutku, padające API bez zatwierdzenia offsetu, nieudany zapis do DLQ też bez zatwierdzenia |
| `internal/obs` | liveness odpowiada przed startem konsumenta, readiness dopiero po, format metryk Prometheusa, odczyt konfiguracji ze zmiennych |

Testy `internal/stream` chodzą po atrapach API i DLQ, więc cała polityka przetwarzania
jest sprawdzana **bez podnoszenia Kafki** — to jest ta część, w której najłatwiej o błąd.

## Stack

| Warstwa | Technologie |
|---|---|
| Język | Go 1.24 |
| Strumień | Apache Kafka 3.9 (KRaft), `segmentio/kafka-go` |
| Obserwowalność | metryki w formacie Prometheusa, logi JSON (`log/slog`) |
| Konteneryzacja | Docker (build wieloetapowy, obraz bez roota), Docker Compose |
| CI | GitHub Actions — gofmt, `go vet`, testy z `-race`, build obu obrazów |

Obraz uruchomieniowy startuje ze statycznego binarium (`CGO_ENABLED=0`) na Alpine,
pod użytkownikiem bez uprawnień roota.
