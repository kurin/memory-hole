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

// Package afs provides an abstract file system, that can be used to write data
// to the backing store.
package afs

import (
	"io"
	"sync"
)

type FileSystem struct {
	uuid string
	wc   writer
}

func (fs *FileSystem) Open(name string) (*File, error) {
	return nil, nil
}

func (fs *FileSystem) Remove(name string) error {
	return nil
}

func (fs *FileSystem) Finalize() (string, error) {
	return "", nil
}

type writer struct {
	meta *metadata
	mux  sync.Mutex
	wc   io.WriteCloser
	pos  int64
}

func (w *writer) write(name string, p []byte) (int, error) {
	w.mux.Lock()
	defer w.mux.Unlock()

	start := w.pos
	n, err := w.wc.Write(p)
	w.pos += int64(n)
	w.meta.update(name, start, int64(n))
	return n, err
}

type metadata struct {
	offsets map[string][][2]int64
}

func (m *metadata) update(name string, pos, off int64) {
	m.offsets[name] = append(m.offsets[name], [2]int64{pos, off})
}
