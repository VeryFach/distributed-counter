package metrics

import (
	"fmt"
	"net/http"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func StartServer(port int) {
	mux := http.NewServeMux()

	mux.Handle(
		"/metrics",
		promhttp.Handler(),
	)

	go func() {
		addr := fmt.Sprintf(":%d", port)

		if err := http.ListenAndServe(addr, mux); err != nil {
			panic(err)
		}
	}()
}