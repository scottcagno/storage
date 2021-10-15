package main

import (
	"fmt"
	"github.com/scottcagno/storage/pkg/util"
	"github.com/scottcagno/storage/pkg/web"
	"github.com/scottcagno/storage/pkg/web/logging"
	"html/template"
	"log"
	"math"
	"math/rand"
	"net/http"
	"path/filepath"
	"strings"
	"time"
)

const LISTENING_ADDR = ":8080"

func main() {

	// initialize a logger
	stdOut, stdErr := logging.NewDefaultLogger()

	// get filepath for later
	path, _ := util.GetFilepath()

	// initialize a new multiplexer configuration
	muxConf := &web.MuxConfig{
		StaticPath:   filepath.Join(path, "data/static/"),
		WithLogging:  true,
		StdOutLogger: stdOut,
		StdErrLogger: stdErr,
	}

	// initialize new http multiplexer
	mux := web.NewServeMux(muxConf)

	// initialize a new template cache configuration
	tmplConf := &web.TemplateConfig{
		StubsPattern:    filepath.Join(path, "data/templates/*/*.html"),
		TemplatePath:    filepath.Join(path, "data/templates/"),
		TemplatePattern: filepath.Join(path, "data/templates/*.html"),
		StdErrLogger:    stdErr,
		FuncMap:         fm,
	}

	// initialize a new template cache instance
	tc, err := web.NewTemplateCache(tmplConf)
	if err != nil {
		log.Panicln(err)
	}
	// add seperate stubs
	err = tc.AddSeparateStubs(tmplConf.StubsPattern)
	if err != nil {
		log.Panicln(err)
	}

	// setup routes and handlers
	//mux.Get("", http.NotFoundHandler())
	mux.Get("/", http.RedirectHandler("/info", http.StatusTemporaryRedirect))
	mux.Get("/index", indexHandler(tc.Lookup("index.html")))

	mux.Get("/index2", index2Handler(tc.Lookup("index-two.html")))

	mux.Get("/home", homeHandler(tc.Lookup("home.html")))

	mux.Get("/login", loginHandler(tc.Lookup("login.html")))
	mux.Get("/post", postHandler(tc.Lookup("post.html")))
	mux.Get("/functest", funcTestHandler(tc.Lookup("func-test.html")))

	// OPTION #1 (passing the entire template cache)
	mux.Get("/user", userHandler(tc))

	// OPTION #2 (passing the single template)
	mux.Get("/user/2", user2Handler(tc.Lookup("user-model-02.html")))

	// OPTION #3 (also, just a different way of passing the single template)
	user3 := tc.Lookup("user-model-03.html")
	mux.Get("/user/3", user3Handler(user3))
	mux.Get("/templates", listTemplates(tc))

	util.HandleSignalInterrupt("Server started, listening on %s", LISTENING_ADDR)
	stdErr.Panicln(http.ListenAndServe(LISTENING_ADDR, mux))
}

var fm = template.FuncMap{
	"add": func(a, b int) int {
		return a + b
	},
	"mod": func(a, b int) int {
		return a % b
	},
	"rand": func(min, max int) int {
		rand.Seed(time.Now().UnixNano())
		return rand.Intn(max-min+1) + min
	},
	"log2": math.Log2,
	"log":  math.Log,
	"love": func(name string) string {
		return "I love you " + name
	},
	"title": strings.ToTitle,
}

func PrintTemplates(name string, tc *web.TemplateCache) {
	templs, defined := tc.Templates()
	fmt.Printf("[%s]\n%s\n", name, defined)
	for i, tmpl := range templs {
		fmt.Printf("template[%d]=%q\n", i, tmpl.Name())
	}
}

func listTemplates(tc *web.TemplateCache) http.Handler {
	fn := func(w http.ResponseWriter, r *http.Request) {
		var s string
		templs, defined := tc.Templates()
		s += fmt.Sprintf("%s\n", defined)
		for i, tmpl := range templs {
			s += fmt.Sprintf("template[%d]=%q\n", i, tmpl.Name())
		}
		fmt.Fprint(w, s)
		return
	}
	return http.HandlerFunc(fn)
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

func index2Handler(t *template.Template) http.Handler {
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

func funcTestHandler(t *template.Template) http.Handler {
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
