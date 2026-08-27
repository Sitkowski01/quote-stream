package alerts

import (
	"encoding/json"
	"net/http"
)

func decodeJSON(r *http.Request, cel any) error {
	defer r.Body.Close()
	return json.NewDecoder(r.Body).Decode(cel)
}
