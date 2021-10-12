package main

import (
	"github.com/scottcagno/storage/pkg/util"
	"github.com/scottcagno/storage/pkg/web"
	"github.com/scottcagno/storage/pkg/web/logging"
	"html/template"
	"log"
	"net/http"
	"path/filepath"
)

const LISTENING_ADDR = ":8080"

func main() {

	// initialize a logger
	stdOut, stdErr := logging.NewDefaultLogger()

	// initialize a new multiplexer configuration
	conf := &web.MuxConfig{
		WithLogging:  true,
		StdOutLogger: stdOut,
		StdErrLogger: stdErr,
	}

	// initialize new http multiplexer
	mux := web.NewServeMux(conf)

	// get filepath for later
	path, _ := util.GetFilepath()

	// initialize new template cache
	tc, err := web.NewTemplateCache0(filepath.Join(path, "data/templates/*.html"), stdErr)
	if err != nil {
		log.Panicln(err)
	}

	// setup routes and handlers
	//mux.Get("", http.NotFoundHandler())
	mux.Get("/", http.RedirectHandler("/info", http.StatusTemporaryRedirect))
	mux.Get("/index", indexHandler(tc.Lookup("index.html")))
	mux.Get("/home", homeHandler(tc.Lookup("home.html")))
	mux.Get("/login", loginHandler(tc.Lookup("login.html")))
	mux.Get("/post", postHandler(tc.Lookup("post.html")))

	// OPTION #1 (passing the entire template cache)
	mux.Get("/user", userHandler(tc))

	// OPTION #2 (passing the single template)
	mux.Get("/user/2", user2Handler(tc.Lookup("user-model-02.html")))

	// OPTION #3 (also, just a different way of passing the single template)
	user3 := tc.Lookup("user-model-03.html")
	mux.Get("/user/3", user3Handler(user3))

	util.HandleSignalInterrupt("Server started, listening on %s", LISTENING_ADDR)
	stdErr.Panicln(http.ListenAndServe(LISTENING_ADDR, mux))
}

func indexHandler(t *template.Template) http.Handler {
	fn := func(w http.ResponseWriter, r *http.Request) {
		err := t.Execute(w, nil)
		if err != nil {
			code := http.StatusNotAcceptable
			http.Error(w, http.StatusText(code), code)
			return
		}
		return
	}
	return http.HandlerFunc(fn)
}

func homeHandler(t *template.Template) http.Handler {
	fn := func(w http.ResponseWriter, r *http.Request) {
		err := t.Execute(w, nil)
		if err != nil {
			code := http.StatusNotAcceptable
			http.Error(w, http.StatusText(code), code)
			return
		}
		return
	}
	return http.HandlerFunc(fn)
}

func loginHandler(t *template.Template) http.Handler {
	fn := func(w http.ResponseWriter, r *http.Request) {
		err := t.Execute(w, nil)
		if err != nil {
			code := http.StatusNotAcceptable
			http.Error(w, http.StatusText(code), code)
			return
		}
		return
	}
	return http.HandlerFunc(fn)
}

func postHandler(t *template.Template) http.Handler {
	fn := func(w http.ResponseWriter, r *http.Request) {
		err := t.Execute(w, nil)
		if err != nil {
			code := http.StatusNotAcceptable
			http.Error(w, http.StatusText(code), code)
			return
		}
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
