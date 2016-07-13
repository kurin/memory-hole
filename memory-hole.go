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
