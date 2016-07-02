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
	"bytes"
	"encoding/binary"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/twinj/uuid"
)

type FileSystem struct {
	uuid string
	w    *writer
	m    *metadata
}

func New(dir string) (*FileSystem, error) {
	u := uuid.NewV4().String()
	f, err := os.Create(filepath.Join(dir, u))
	if err != nil {
		return nil, err
	}
	m := &metadata{
		Offsets: make(map[string][][2]int64),
	}
	return &FileSystem{
		uuid: u,
		m:    m,
		w: &writer{
			wc:   f,
			meta: m,
		},
	}, nil
}

func (fs *FileSystem) Open(name string) (*File, error) {
	return &File{
		name: name,
		w:    fs.w,
	}, nil
}

func (fs *FileSystem) Remove(name string) error {
	return nil
}

func (fs *FileSystem) Finalize() (string, error) {
	enc := json.NewEncoder(fs.w.wc)
	if err := enc.Encode(fs.m); err != nil {
		fs.w.wc.Close()
		return "", err
	}
	b := make([]byte, 10)
	binary.PutVarint(b, fs.w.pos)
	if _, err := io.Copy(fs.w.wc, bytes.NewBuffer(b)); err != nil {
		fs.w.wc.Close()
		return "", err
	}
	if err := fs.w.wc.Close(); err != nil {
		return "", err
	}
	return fs.uuid, nil
}

// TODO: move this all into fs
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
	Offsets map[string][][2]int64
}

func (m *metadata) update(name string, pos, off int64) {
	m.Offsets[name] = append(m.Offsets[name], [2]int64{pos, off})
}
