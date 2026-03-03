package binding

import (
	"encoding/xml"
	"net/http"

	"github.com/pkg/errors"
)

type xmlBinding struct{}

func (xmlBinding) Name() string {
	return "xml"
}

func (xmlBinding) Bind(req *http.Request, obj interface{}) error {
	req.Body = http.MaxBytesReader(nil, req.Body, DefaultMaxBodyBytes)
	decoder := xml.NewDecoder(req.Body)
	if err := decoder.Decode(obj); err != nil {
		return errors.WithStack(err)
	}
	return validate(obj)
}
