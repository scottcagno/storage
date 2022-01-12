package web

import (
	"net/http"
)

func IfOK(next http.Handler, ok bool) http.Handler {
	fn := func(w http.ResponseWriter, r *http.Request) {
		if !ok {
			code := http.StatusMethodNotAllowed
			http.Error(w, http.StatusText(code), code)
			return
		}
		next.ServeHTTP(w, r)
	}
	return http.HandlerFunc(fn)
}

// StaticHandler is just a static folder handler helper
func StaticHandler(prefix, path string) http.Handler {
	return http.StripPrefix(prefix, http.FileServer(http.Dir(path)))
}

// Middleware is a piece of middleware.
type Middleware func(http.Handler) http.Handler

// Chain acts as a list of http.Handler middlewares. It's effectively immutable.
type Chain struct {
	mw []Middleware
}

// NewChain creates a new chain, memorizing the given list of middleware handlers.
// New serves no other function, middlewares are only called upon a call to Then().
func NewChain(mw ...Middleware) *Chain {
	return &Chain{
		mw: append(([]Middleware)(nil), mw...),
	}
}

// Then chains the middleware and returns the final http.Handler.
// Then() treats nil as http.DefaultServeMux.
func (c *Chain) Then(handler http.Handler) http.Handler {
	if handler == nil {
		handler = http.DefaultServeMux
	}
	for i := range c.mw {
		handler = c.mw[len(c.mw)-1-i](handler)
	}
	return handler
}

// ThenFunc works identically to Then, but takes
// a HandlerFunc instead of a Handler.
func (c *Chain) ThenFunc(handler http.HandlerFunc) http.Handler {
	if handler == nil {
		return c.Then(nil)
	}
	return c.Then(handler)
}

// Append extends a chain, adding the specified constructors
// as the last ones in the request flow.
func (c *Chain) Append(mw ...Middleware) *Chain {
	nc := make([]Middleware, 0, len(c.mw)+len(mw))
	nc = append(nc, c.mw...)
	nc = append(nc, mw...)

	return &Chain{
		mw: nc,
	}
}

// Extend extends a chain by adding the specified chain
// as the last one in the request flow.
func (c *Chain) Extend(chain *Chain) *Chain {
	return c.Append(chain.mw...)
}
