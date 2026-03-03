// Copyright 2022 Teamgram Authors
//  All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Author: teamgramio (teamgram.io@gmail.com)
//

package http_util

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path"
	"strings"

	"github.com/teamgram/marmota/pkg/logx"
	"github.com/teamgram/marmota/pkg/net/http/binding"
)

// DefaultMaxUploadBytes is the default maximum size in bytes for a single uploaded file (32MB).
// Use GetUploadFileWithLimit to override per call.
const DefaultMaxUploadBytes = 32 << 20

var ErrUploadTooLarge = errors.New("upload file exceeds maximum allowed size")
var ErrInvalidFileName = errors.New("upload file name contains invalid characters")

type HttpApiRequest interface {
	Method() string
}

type HttpApiMethod interface {
	NewRequest() HttpApiRequest
	Decode2(c *http.Request, contentType string, r HttpApiRequest) (err error)
}

func BindWithApiRequest(r *http.Request, req HttpApiMethod) error {
	var (
		b           binding.Binding
		contentType = r.Header.Get("Content-Type")
		logger      = logx.WithContext(r.Context())
	)

	if r.Method == "GET" {
		b = binding.Form
	} else {
		var stripContentTypeParam = func(contentType string) string {
			i := strings.Index(contentType, ";")
			if i != -1 {
				contentType = contentType[:i]
			}
			return contentType
		}

		contentType = stripContentTypeParam(contentType)
		switch contentType {
		case binding.MIMEJSON:
			// b = binding.FASTJSON
			b = binding.JSON
		case binding.MIMEMultipartPOSTForm:
			b = binding.FormMultipart
		case binding.MIMEPOSTForm:
			b = binding.FormPost
		default:
			return fmt.Errorf("not support contentType: %s", contentType)
		}
	}

	bindingReq := req.NewRequest()
	err := b.Bind(r, bindingReq)
	if err != nil {
		logger.Errorf("bind form error: %v", err)
		return err
	} else {
		logger.Debugf("bindingReq(%s): %s", bindingReq.Method(), logx.DebugLogString(bindingReq))
	}

	if err = req.Decode2(r, contentType, bindingReq); err != nil {
		logger.Errorf("decode(%s) error: %v", bindingReq.Method(), err)
		return err
	}

	logger.Infof("req(%s): %s", bindingReq.Method(), logx.DebugLogString(req))

	return nil
}

// GetUploadFile reads the first multipart form file for key and returns its
// sanitized filename and body, with a default max size of DefaultMaxUploadBytes.
func GetUploadFile(c *http.Request, key string) (fileName string, file []byte, err error) {
	return GetUploadFileWithLimit(c, key, DefaultMaxUploadBytes)
}

// GetUploadFileWithLimit reads the first multipart form file for key and returns
// its sanitized filename and body. Files larger than maxBytes return ErrUploadTooLarge.
// The returned fileName is the base name only; path traversal and null bytes are rejected.
func GetUploadFileWithLimit(c *http.Request, key string, maxBytes int64) (fileName string, file []byte, err error) {
	file2, fileHeader, err2 := c.FormFile(key)
	if err2 != nil {
		err = err2
		return
	}
	defer file2.Close()

	rawName := fileHeader.Filename
	fileName = path.Base(rawName)
	if fileName == "" || fileName == "." {
		fileName = rawName
	}
	if strings.Contains(fileName, "\x00") || strings.Contains(fileName, "..") {
		err = ErrInvalidFileName
		return
	}

	limited := io.LimitReader(file2, maxBytes+1)
	buf := new(bytes.Buffer)
	n, err := io.Copy(buf, limited)
	if err != nil {
		return
	}
	if n > maxBytes {
		err = ErrUploadTooLarge
		return
	}

	file = buf.Bytes()
	return
}
