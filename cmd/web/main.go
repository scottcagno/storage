package main

import (
	"fmt"
	"github.com/scottcagno/storage/pkg/util"
	"github.com/scottcagno/storage/pkg/web"
	"html/template"
	"log"
	"net/http"
	"path/filepath"
)

const LISTENING_ADDR = ":8080"

func main() {

	// get filepath for later
	path, _ := util.GetFilepath()

	// init new http multiplexer
	mux := web.NewServeMux(&web.MuxConfig{WithLogging: true})

	// init new template cache
	tc, err := web.NewTemplateCache0(filepath.Join(path, "data/templates/*.html"), nil)
	if err != nil {
		log.Panicln(err)
	}

	// setup routes and handlers
	mux.Get("/", http.NotFoundHandler())
	mux.Get("/home", homeHandler())

	// OPTION #1 (passing the entire template cache)
	mux.Get("/user", userHandler(tc))

	// OPTION #2 (passing the single template)
	mux.Get("/user/2", user2Handler(tc.Lookup("user-model-02.html")))

	// OPTION #3 (also, just a different way of passing the single template)
	user3 := tc.Lookup("user-model-03.html")
	mux.Get("/user/3", user3Handler(user3))

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

func userHandler(t *web.TemplateCache) http.Handler {
	user := map[string]interface{}{
		"ID":       34,
		"Name":     "Jon Doe",
		"Email":    "jdoe@ex.com",
		"Password": "none",
		"IsActive": true,
	}
	fn := func(w http.ResponseWriter, r *http.Request) {
		t.Render(w, r, "user-model.html", struct {
			User map[string]interface{}
		}{
			User: user,
		})
		return
	}
	return http.HandlerFunc(fn)
}

func user2Handler(t *template.Template) http.Handler {
	user := map[string]interface{}{
		"ID":       36,
		"Name":     "Jane Doe (the explorer)",
		"Email":    "jdoe@ex.com",
		"Password": "none",
		"IsActive": true,
	}
	fn := func(w http.ResponseWriter, r *http.Request) {
		err := t.Execute(w, struct {
			User map[string]interface{}
		}{
			User: user,
		})
		if err != nil {
			code := http.StatusNotAcceptable
			http.Error(w, http.StatusText(code), code)
			return
		}
		return
	}
	return http.HandlerFunc(fn)
}

func user3Handler(t *template.Template) http.Handler {
	user := map[string]interface{}{
		"ID":       86,
		"Name":     "Some Rando",
		"Email":    "srando@example.com",
		"Password": "foobar",
		"IsActive": true,
	}
	fn := func(w http.ResponseWriter, r *http.Request) {
		err := t.Execute(w, struct {
			User map[string]interface{}
		}{
			User: user,
		})
		if err != nil {
			code := http.StatusNotAcceptable
			http.Error(w, http.StatusText(code), code)
			return
		}
		return
	}
	return http.HandlerFunc(fn)
}
