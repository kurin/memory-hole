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
	"os"
	"path/filepath"
)

type File struct {
	*os.File
	name string
}

func (f *File) Stat() (os.FileInfo, error) {
	s, err := f.File.Stat()
	if err != nil {
		return nil, err
	}
	return &fileInfo{
		FileInfo: s,
		name:     filepath.Base(f.name),
	}, nil
}

type fileInfo struct {
	os.FileInfo
	name string
}

func (fi *fileInfo) Name() string {
	return fi.name
}
