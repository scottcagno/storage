package web

import (
	"bytes"
	"fmt"
	"html/template"
	"log"
	"mime"
	"net/http"
	"sync"
)

var fm = template.FuncMap{}

type TemplateCache struct {
	cache  *template.Template
	logger *log.Logger
	buff   sync.Pool
	sync.RWMutex
}

func NewTemplateCache(pattern string, logger *log.Logger) *TemplateCache {
	//example pattern: "web/templates/*.html"
	return &TemplateCache{
		cache:  template.Must(template.New("*").Funcs(fm).ParseGlob(pattern)),
		logger: logger,
		buff: sync.Pool{
			New: func() interface{} {
				return new(bytes.Buffer)
			},
		},
	}
}

func (t *TemplateCache) Render(w http.ResponseWriter, r *http.Request, tmpl string, data interface{}) {
	t.Lock()
	defer t.Unlock()
	buffer := t.buff.Get().(*bytes.Buffer)
	buffer.Reset()
	err := t.cache.ExecuteTemplate(buffer, tmpl, data)
	if err != nil {
		t.buff.Put(buffer)
		t.logger.Printf("Error while executing template (%s): %v\n", tmpl, err)
		http.Redirect(w, r, "/error/404", http.StatusTemporaryRedirect)
		return
	}
	_, err = buffer.WriteTo(w)
	if err != nil {
		t.logger.Printf("Error while writing (Render) to ResponseWriter: %v\n", err)
	}
	t.buff.Put(buffer)
	return
}

func (t *TemplateCache) Raw(w http.ResponseWriter, format string, data ...interface{}) {
	t.Lock()
	defer t.Unlock()
	_, err := fmt.Fprintf(w, format, data...)
	if err != nil {
		t.logger.Printf("Error while writing (Raw) to ResponseWriter: %v\n", err)
		return
	}
	return
}

func (t *TemplateCache) ContentType(w http.ResponseWriter, content string) {
	t.Lock()
	defer t.Unlock()
	ct := mime.TypeByExtension(content)
	if ct == "" {
		t.logger.Printf("Error, incompatible content type!\n")
		return
	}
	w.Header().Set("Content-Type", ct)
	return
}
