package web

import (
	"fmt"
	"github.com/scottcagno/storage/pkg/web/logging"
	"html/template"
	"io"
	"log"
	"mime"
	"net/http"
	"os"
	"path/filepath"
)

var (
	defaultTemplatePattern = "web/templates/*.html"
	defaultStubsPattern    = "web/templates/*/*.html"
)

type TemplateConfig struct {
	StubsPattern    string
	TemplatePattern string
	StdErrLogger    *log.Logger
	FuncMap         template.FuncMap
}

func checkTemplateConfig(conf *TemplateConfig) *TemplateConfig {
	if conf == nil {
		conf = &TemplateConfig{
			TemplatePattern: defaultTemplatePattern,
			StdErrLogger:    logging.NewStdErrLogger(os.Stderr),
			FuncMap:         template.FuncMap{},
		}
	}
	if conf.TemplatePattern == *new(string) {
		conf.TemplatePattern = defaultTemplatePattern
	}
	if conf.StdErrLogger == nil {
		conf.StdErrLogger = logging.NewStdErrLogger(os.Stderr)
	}
	return conf
}

// TemplateCache is a template engine that caches golang html/template files
type TemplateCache struct {
	conf  *TemplateConfig
	cache *template.Template
}

// NewTemplateCache takes a pattern to glob and an optional logger and returns a
// new *TemplateCache instance. On success, it returns a nil error. An example
// pattern to glob would be: "web/templates/*.html" or "my-path/*.tmpl.html"
func NewTemplateCache(conf *TemplateConfig) (*TemplateCache, error) {
	conf = checkTemplateConfig(conf)
	t, err := template.New("*").Funcs(conf.FuncMap).ParseGlob(conf.TemplatePattern)
	if err != nil {
		return nil, err
	}
	tc := &TemplateCache{
		cache: t,
		conf:  checkTemplateConfig(conf),
	}
	return tc, nil
}

func NewTemplateCacheWithSeperateStubs(conf *TemplateConfig) (*TemplateCache, error) {
	conf = checkTemplateConfig(conf)
	t, err := template.New("*").Funcs(conf.FuncMap).ParseGlob(conf.TemplatePattern)
	if err != nil {
		return nil, err
	}
	if matches, _ := filepath.Glob(conf.StubsPattern); len(matches) > 0 {
		t, err = t.ParseGlob(conf.StubsPattern)
		if err != nil {
			return nil, err
		}
	}
	tc := &TemplateCache{
		cache: t,
		conf:  checkTemplateConfig(conf),
	}
	return tc, nil
}

func (t *TemplateCache) AddSeparateStubs(stubsPattern string) error {
	var err error
	if matches, _ := filepath.Glob(stubsPattern); len(matches) > 0 {
		t.cache, err = t.cache.ParseGlob(stubsPattern)
		if err != nil {
			return err
		}
	}
	return nil
}

func (t *TemplateCache) Templates() ([]*template.Template, string) {
	return t.cache.Templates(), t.cache.DefinedTemplates()
}

func (t *TemplateCache) Use(name string) *template.Template {
	return t.cache.Lookup(name)
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
		t.conf.StdErrLogger.Printf("Error while executing template (%s): %v\n", tmpl, err)
		http.Redirect(w, r, "/error/404", http.StatusTemporaryRedirect)
		return
	}
	_, err = buffer.WriteTo(w)
	if err != nil {
		t.conf.StdErrLogger.Printf("Error while writing (Render) to ResponseWriter: %v\n", err)
	}
	bufPool.Put(buffer)
	return
}

func (t *TemplateCache) Render(w http.ResponseWriter, r *http.Request, tmpl string, data interface{}) {
	err := t.cache.ExecuteTemplate(w, tmpl, data)
	if err != nil {
		t.conf.StdErrLogger.Printf("Error while executing template (%s): %v\n", tmpl, err)
		http.Redirect(w, r, "/error/404", http.StatusTemporaryRedirect)
		return
	}
	if err != nil {
		t.conf.StdErrLogger.Printf("Error while writing (Render) to ResponseWriter: %v\n", err)
	}
	return
}

func (t *TemplateCache) Raw(w http.ResponseWriter, format string, data ...interface{}) {
	_, err := fmt.Fprintf(w, format, data...)
	if err != nil {
		t.conf.StdErrLogger.Printf("Error while writing (Raw) to ResponseWriter: %v\n", err)
		return
	}
	return
}

func (t *TemplateCache) ContentType(w http.ResponseWriter, content string) {
	ct := mime.TypeByExtension(content)
	if ct == "" {
		t.conf.StdErrLogger.Printf("Error, incompatible content type!\n")
		return
	}
	w.Header().Set("Content-Type", ct)
	return
}
