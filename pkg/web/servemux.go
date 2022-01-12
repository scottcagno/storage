package web

import (
	"fmt"
	"github.com/scottcagno/storage/pkg/web/logging"
	"log"
	"mime"
	"net/http"
	"os"
	"path/filepath"
	"runtime/debug"
	"sort"
	"strings"
	"sync"
	"time"
)

type muxEntry struct {
	method  string
	pattern string
	handler http.Handler
}

func (m muxEntry) String() string {
	if m.method == http.MethodGet {
		return fmt.Sprintf("[%s]&nbsp;&nbsp;&nbsp;&nbsp;<a href=\"%s\">%s</a>", m.method, m.pattern, m.pattern)
	}
	if m.method == http.MethodPost {
		return fmt.Sprintf("[%s]&nbsp;&nbsp;&nbsp;%s", m.method, m.pattern)
	}
	if m.method == http.MethodPut {
		return fmt.Sprintf("[%s]&nbsp;&nbsp;&nbsp;&nbsp;%s", m.method, m.pattern)
	}
	if m.method == http.MethodDelete {
		return fmt.Sprintf("[%s]&nbsp;%s", m.method, m.pattern)
	}
	return fmt.Sprintf("[%s]&nbsp;%s", m.method, m.pattern)
}

func (s *ServeMux) Len() int {
	return len(s.es)
}

func (s *ServeMux) Less(i, j int) bool {
	return s.es[i].pattern < s.es[j].pattern
}

func (s *ServeMux) Swap(i, j int) {
	s.es[j], s.es[i] = s.es[i], s.es[j]
}

func (s *ServeMux) Search(x string) int {
	return sort.Search(len(s.es), func(i int) bool {
		return s.es[i].pattern >= x
	})
}

var (
	defaultStaticPath = "web/static/"

	DefaultMuxConfigMaxOpts = &MuxConfig{
		StaticPath:   defaultStaticPath,
		WithLogging:  true,
		StdOutLogger: logging.NewStdOutLogger(os.Stdout),
		StdErrLogger: logging.NewStdErrLogger(os.Stderr),
	}

	defaultMuxConfigMinOpts = &MuxConfig{
		StaticPath: defaultStaticPath,
	}
)

type MuxConfig struct {
	StaticPath   string
	WithLogging  bool
	StdOutLogger *log.Logger
	StdErrLogger *log.Logger
}

func checkMuxConfig(conf *MuxConfig) *MuxConfig {
	if conf == nil {
		conf = defaultMuxConfigMinOpts
	}
	if conf.StaticPath == *new(string) {
		conf.StaticPath = defaultStaticPath
	} else {
		conf.StaticPath = filepath.FromSlash(conf.StaticPath + string(filepath.Separator))
	}
	if conf.WithLogging {
		if conf.StdOutLogger == nil {
			conf.StdOutLogger = logging.NewStdOutLogger(os.Stdout)
		}
		if conf.StdErrLogger == nil {
			conf.StdErrLogger = logging.NewStdErrLogger(os.Stderr)
		}
	}
	return conf
}

type ServeMux struct {
	lock   sync.Mutex
	logger *logging.LevelLogger
	em     map[string]muxEntry
	es     []muxEntry
}

func NewServeMux(logger *logging.LevelLogger) *ServeMux {
	mux := &ServeMux{
		em: make(map[string]muxEntry),
		es: make([]muxEntry, 0),
	}
	if logger != nil {
		mux.logger = logger
	}
	mux.Get("/favicon.ico", http.NotFoundHandler())
	mux.Get("/info", mux.info())
	return mux
}

func (s *ServeMux) Handle(method string, pattern string, handler http.Handler) {
	s.lock.Lock()
	defer s.lock.Unlock()
	if pattern == "" {
		panic("http: invalid pattern")
	}
	if handler == nil {
		panic("http: nil handler")
	}
	if _, exist := s.em[pattern]; exist {
		panic("http: multiple registrations for " + pattern)
	}
	entry := muxEntry{
		method:  method,
		pattern: pattern,
		handler: handler,
	}
	s.em[pattern] = entry
	if pattern[len(pattern)-1] == '/' {
		s.es = appendSorted(s.es, entry)
	}
	//s.routes.Put(entry)
}

func appendSorted(es []muxEntry, e muxEntry) []muxEntry {
	n := len(es)
	i := sort.Search(n, func(i int) bool {
		return len(es[i].pattern) < len(e.pattern)
	})
	if i == n {
		return append(es, e)
	}
	// we now know that i points at where we want to insert
	es = append(es, muxEntry{}) // try to grow the slice in place, any entry works.
	copy(es[i+1:], es[i:])      // Move shorter entries down
	es[i] = e
	return es
}

func (s *ServeMux) HandleFunc(method, pattern string, handler func(http.ResponseWriter, *http.Request)) {
	if handler == nil {
		panic("http: nil handler")
	}
	s.Handle(method, pattern, http.HandlerFunc(handler))
}

func (s *ServeMux) Forward(oldpattern string, newpattern string) {
	s.Handle(http.MethodGet, oldpattern, http.RedirectHandler(newpattern, http.StatusTemporaryRedirect))
}

func (s *ServeMux) Get(pattern string, handler http.Handler) {
	s.Handle(http.MethodGet, pattern, handler)
}

func (s *ServeMux) Post(pattern string, handler http.Handler) {
	s.Handle(http.MethodPost, pattern, handler)
}

func (s *ServeMux) Put(pattern string, handler http.Handler) {
	s.Handle(http.MethodPut, pattern, handler)
}

func (s *ServeMux) Delete(pattern string, handler http.Handler) {
	s.Handle(http.MethodDelete, pattern, handler)
}

func (s *ServeMux) Static(pattern string, path string) {
	staticHandler := http.StripPrefix(pattern, http.FileServer(http.Dir(path)))
	s.Handle(http.MethodGet, pattern, staticHandler)
}

func (s *ServeMux) getEntries() []string {
	s.lock.Lock()
	defer s.lock.Unlock()
	var entries []string
	for _, entry := range s.em {
		entries = append(entries, fmt.Sprintf("%s %s\n", entry.method, entry.pattern))
	}
	return entries
}

func (s *ServeMux) matchEntry(path string) (string, string, http.Handler) {
	e, ok := s.em[path]
	if ok {
		return e.method, e.pattern, e.handler
	}
	for _, e = range s.es {
		if strings.HasPrefix(path, e.pattern) {
			return e.method, e.pattern, e.handler
		}
	}
	return "", "", nil
}

func (s *ServeMux) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// check for post request in order to validate via the referer
	if r.Method == "POST" && !strings.Contains(r.Referer(), r.Host) {
		// possibly add origin check in there too...
		//
		code := http.StatusForbidden // invalid request redirect to 403
		http.Error(w, http.StatusText(code), code)
		return
	}
	// look for a matching entry
	m, _, h := s.matchEntry(r.URL.Path)
	if m != r.Method || h == nil {
		// otherwise, return not found
		h = http.NotFoundHandler()
	}
	// if logging is configured, then log, otherwise skip
	if s.logger != nil {
		h = s.requestLogger(h)
	}
	// serve
	h.ServeHTTP(w, r)
}

func (s *ServeMux) info() http.Handler {
	fn := func(w http.ResponseWriter, r *http.Request) {
		var data []string
		data = append(data, fmt.Sprintf("<h3>Registered Routes (%d)</h3>", len(s.em)))
		for _, entry := range s.em {
			data = append(data, entry.String())
		}
		sort.Slice(data, func(i, j int) bool {
			return data[i] < data[j]
		})
		s.ContentType(w, ".html")
		_, err := fmt.Fprintf(w, strings.Join(data, "<br>"))
		if err != nil {
			code := http.StatusInternalServerError
			http.Error(w, http.StatusText(code), code)
			return
		}
		return
	}
	return http.HandlerFunc(fn)
}

func (s *ServeMux) ContentType(w http.ResponseWriter, content string) {
	s.lock.Lock()
	defer s.lock.Unlock()
	ct := mime.TypeByExtension(content)
	if ct == "" {
		s.logger.Error("Error, incompatible content type!\n")
		return
	}
	w.Header().Set("Content-Type", ct)
	return
}

type responseData struct {
	status int
	size   int
}

type loggingResponseWriter struct {
	http.ResponseWriter
	data *responseData
}

func (w *loggingResponseWriter) Header() http.Header {
	return w.ResponseWriter.Header()
}

func (w *loggingResponseWriter) Write(b []byte) (int, error) {
	size, err := w.ResponseWriter.Write(b)
	w.data.size += size
	return size, err
}

func (w *loggingResponseWriter) WriteHeader(statusCode int) {
	w.ResponseWriter.WriteHeader(statusCode)
	w.data.status = statusCode
}

func (s *ServeMux) requestLogger(next http.Handler) http.Handler {
	fn := func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if err := recover(); err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				s.logger.Error("err: %v, trace: %s\n", err, debug.Stack())
			}
		}()
		lrw := loggingResponseWriter{
			ResponseWriter: w,
			data: &responseData{
				status: 200,
				size:   0,
			},
		}
		next.ServeHTTP(&lrw, r)
		if 400 <= lrw.data.status && lrw.data.status <= 599 {
			str, args := logRequest(lrw.data.status, r)
			s.logger.Error(str, args...)
			return
		}
		str, args := logRequest(lrw.data.status, r)
		s.logger.Info(str, args...)
		return
	}
	return http.HandlerFunc(fn)
}

func logRequest(code int, r *http.Request) (string, []interface{}) {
	format, values := "# %s - - [%s] \"%s %s %s\" %d %d\n", []interface{}{
		r.RemoteAddr,
		time.Now().Format(time.RFC1123Z),
		r.Method,
		r.URL.EscapedPath(),
		r.Proto,
		code,
		r.ContentLength,
	}
	return format, values
}
