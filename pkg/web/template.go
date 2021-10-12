package web

import (
	"fmt"
	"html/template"
	"io"
	"log"
	"mime"
	"net/http"
	"path/filepath"
)

var fm = template.FuncMap{}

type TemplateCache struct {
	cache  *template.Template
	logger *log.Logger
}

// NewTemplateCache0 takes a pattern to glob and an optional logger and returns a
// new *TemplateCache instance. On success, it returns a nil error. An example
// pattern to glob would be: "web/templates/*.html" or "my-path/*.tmpl.html"
func NewTemplateCache0(pattern string, logger *log.Logger) (*TemplateCache, error) {
	t, err := template.New("*").Funcs(fm).ParseGlob(pattern)
	if err != nil {
		return nil, err
	}
	tc := &TemplateCache{
		cache:  t,
		logger: logger,
	}
	return tc, nil
}

// NewTemplateCache1 takes a pattern to glob and an optional logger and returns a
// new *TemplateCache instance. On success, it returns a nil error. An example
// pattern to glob would be: "web/templates/*.html" or "my-path/*.tmpl.html"
func NewTemplateCache1(pattern string, logger *log.Logger) (*TemplateCache, error) {
	matches, err := filepath.Glob(pattern)
	if err != nil {
		return nil, err
	}
	t, err := template.ParseFiles(matches...)
	if err != nil {
		return nil, err
	}
	t = t.Funcs(fm)
	tc := &TemplateCache{
		cache:  template.Must(template.New("*").Funcs(fm).ParseGlob(pattern)),
		logger: logger,
	}
	return tc, nil
}

func (t *TemplateCache) Lookup(name string) *template.Template {
	return t.cache.Lookup(name)
}

func (t *TemplateCache) ExecuteTemplate(w io.Writer, name string, data interface{}) error {
	err := t.cache.ExecuteTemplate(w, name, data)
	if err != nil {
		return err
	}
	return nil
}

func (t *TemplateCache) RenderWithBuffer(w http.ResponseWriter, r *http.Request, tmpl string, data interface{}) {
	bufPool := OpenBufferPool()
	buffer := bufPool.Get()
	err := t.cache.ExecuteTemplate(buffer, tmpl, data)
	if err != nil {
		bufPool.Put(buffer)
		t.logger.Printf("Error while executing template (%s): %v\n", tmpl, err)
		http.Redirect(w, r, "/error/404", http.StatusTemporaryRedirect)
		return
	}
	_, err = buffer.WriteTo(w)
	if err != nil {
		t.logger.Printf("Error while writing (Render) to ResponseWriter: %v\n", err)
	}
	bufPool.Put(buffer)
	return
}

func (t *TemplateCache) Render(w http.ResponseWriter, r *http.Request, tmpl string, data interface{}) {
	err := t.cache.ExecuteTemplate(w, tmpl, data)
	if err != nil {
		t.logger.Printf("Error while executing template (%s): %v\n", tmpl, err)
		http.Redirect(w, r, "/error/404", http.StatusTemporaryRedirect)
		return
	}
	if err != nil {
		t.logger.Printf("Error while writing (Render) to ResponseWriter: %v\n", err)
	}
	return
}

func (t *TemplateCache) Raw(w http.ResponseWriter, format string, data ...interface{}) {
	_, err := fmt.Fprintf(w, format, data...)
	if err != nil {
		t.logger.Printf("Error while writing (Raw) to ResponseWriter: %v\n", err)
		return
	}
	return
}

func (t *TemplateCache) ContentType(w http.ResponseWriter, content string) {
	ct := mime.TypeByExtension(content)
	if ct == "" {
		t.logger.Printf("Error, incompatible content type!\n")
		return
	}
	w.Header().Set("Content-Type", ct)
	return
}
