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
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
)

type node struct {
	dir  bool
	mux  sync.Mutex
	sub  map[string]*node
	uuid string
}

func (n *node) mkdir(name string) (*node, error) {
	if !n.dir {
		return nil, errors.New("not a directory")
	}
	s := strings.TrimPrefix(name, "/")
	i := strings.Index(s, "/")
	var rest string
	if i > -1 {
		rest = s[i:]
		s = s[:i]
	}
	n.mux.Lock()
	defer n.mux.Unlock()
	if n.sub == nil {
		n.sub = make(map[string]*node)
	}
	if rest == "" {
		g, ok := n.sub[s]
		if ok && !g.dir {
			return nil, fmt.Errorf("%s: already exists", name)
		}
		if ok {
			return g, nil
		}
		end := &node{dir: true}
		n.sub[s] = end
		return end, nil
	}
	if _, ok := n.sub[s]; !ok {
		n.sub[s] = &node{dir: true}
	}
	return n.sub[s].mkdir(rest)
}

func (n *node) add(name string) (*node, error) {
	path := filepath.Dir(name)
	base := filepath.Base(name)
	dir, err := n.mkdir(path)
	if err != nil {
		return nil, err
	}
	dir.mux.Lock()
	defer dir.mux.Unlock()
	if dir.sub == nil {
		dir.sub = make(map[string]*node)
	}
	end := &node{}
	dir.sub[base] = end
	return end, nil
}

func (n *node) get(name string) (*node, error) {
	n.mux.Lock()
	defer n.mux.Unlock()

	s := strings.TrimPrefix(name, "/")
	i := strings.Index(s, "/")
	var rest string
	if i > -1 {
		rest = s[i:]
		s = s[:i]
	}

	g, ok := n.sub[s]
	if !ok {
		return nil, fmt.Errorf("%s: no such file or directory", name)
	}
	if rest == "" {
		return g, nil
	}
	return g.get(rest)
}

func (n *node) remove(name string, rmdir bool) (*node, error) {
	dir := filepath.Dir(name)
	d, err := n.get(dir)
	if err != nil {
		return nil, err
	}

	d.mux.Lock()
	defer d.mux.Unlock()

	sd, ok := d.sub[filepath.Base(name)]
	if !ok {
		return nil, fmt.Errorf("%s: no such file or directory", name)
	}
	sd.mux.Lock()
	defer sd.mux.Unlock()

	if sd.dir && !rmdir {
		return nil, fmt.Errorf("%s: is a directory")
	}

	if len(sd.sub) > 0 {
		return nil, fmt.Errorf("%s: directory not empty")
	}

	delete(d.sub, filepath.Base(name))
	return sd, nil
}
