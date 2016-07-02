// Copyright 2016 Google
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package afs

import (
	"io"
	"os"
	"time"
)

type File struct {
	name string
	w    *writer
}

func (f *File) Stat() (os.FileInfo, error) {
	return f, nil
}

func (f *File) Write(b []byte) (int, error) {
	return f.w.write(f.name, b)
}

func (f *File) Read(b []byte) (int, error) {
	return 0, io.EOF
}

func (f *File) Close() error {
	return nil
}

func (f *File) Name() string {
	return ""
}

func (f *File) Size() int64 {
	return 0
}

func (f *File) Mode() os.FileMode {
	return 0777
}

func (f *File) ModTime() time.Time {
	return time.Time{}
}

func (f *File) IsDir() bool {
	return false
}

func (f *File) Sys() interface{} {
	return nil
}
