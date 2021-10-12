package web

import "errors"

var (
	ErrBadStaticFilepath   = errors.New("bad static filepath")
	ErrBadTemplateFilepath = errors.New("bad template filepath")
	ErrBadTemplateSuffix   = errors.New("bad template suffix")
)
