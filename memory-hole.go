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

package main

import (
	"log"
	"os"
	"os/signal"

	"bazil.org/fuse"

	"github.com/kurin/memory-hole/cfs"
)

func main() {
	fs := cfs.FS{
		Mountpoint: "/tmp/mnt",
		WorkDir:    "/tmp/stuff",
	}
	ch := make(chan os.Signal)
	signal.Notify(ch, os.Interrupt)
	go func() {
		for {
			<-ch
			if err := fuse.Unmount(fs.Mountpoint); err != nil {
				log.Printf("unmount: %v", err)
			}
		}
	}()
	if err := fs.Mount(); err != nil {
		log.Printf("mount: %v", err)
	}
	defer fs.Close()
}
