package web

import (
	"fmt"
	"net/http"
)

var (
	ContentTypeTextHTML       = "text/html"
	ContentTypeTextPlain      = "text/plain"
	ContentTypeTextCSS        = "text/css"
	ContentTypeTextCSV        = "text/csv"
	ContentTypeTextJavaScript = "text/javascript"

	ContentTypeApplicationXML         = "application/xml"
	ContentTypeApplicationZip         = "application/zip"
	ContentTypeApplicationPDF         = "application/pdf"
	ContentTypeApplicationJSON        = "application/json"
	ContentTypeApplicationJavaScript  = "application/javascript"
	ContentTypeApplicationOctetStream = "application/octet-stream"

	ContentTypeImageAPNG = "image/apng"
	ContentTypeImageGIF  = "image/gif"
	ContentTypeImageJPEG = "image/jpeg"
	ContentTypeImagePNG  = "image/png"
	ContentTypeSVGXML    = "image/svg+xml"
	ContentTypeWebP      = "image/webp"

	ContentTypeMultiPartFormData = "multipart/form-data"
)

func ContentType(w http.ResponseWriter, ct string) {
	w.Header().Set("Content-Type", fmt.Sprintf("%s; charset=utf-8", ct))
}
