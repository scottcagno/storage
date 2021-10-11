package main

import (
	"fmt"
	"github.com/scottcagno/storage/pkg/util"
	"github.com/scottcagno/storage/pkg/web"
	"log"
	"net/http"
)

const LISTENING_ADDR = ":8080"

func main() {

	mux := web.NewServeMux(&web.MuxConfig{WithLogging: true})
	mux.Get("/", http.NotFoundHandler())
	mux.Get("/home", homeHandler())

	util.HandleSignalInterrupt("Server started, listening on %s", LISTENING_ADDR)
	log.Println(http.ListenAndServe(LISTENING_ADDR, mux))
}

func homeHandler() http.Handler {
	fn := func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "you are home")
		return
	}
	return http.HandlerFunc(fn)
}

func homePlusHandler() http.Handler {
	fn := func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "you are at home plus!")
		return
	}
	return http.HandlerFunc(fn)
}

func foobar() http.Handler {
	fn := func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "you are at: %s", r.URL.Path)
		return
	}
	return http.HandlerFunc(fn)
}
