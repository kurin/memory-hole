package afs

import (
	"bytes"
	"io"
	"io/ioutil"
	"os"
	"testing"
)

func TestWriter(t *testing.T) {
	dir, err := ioutil.TempDir("", "afs")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	t.Log(dir)
	fs, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	f, err := fs.Open("/a/file/I/like")
	if err != nil {
		t.Fatal(err)
	}
	buf := &bytes.Buffer{}
	buf.WriteString("loopy\n")
	if _, err := io.Copy(f, buf); err != nil {
		t.Fatal(err)
	}
	uuid, err := fs.Finalize()
	if err != nil {
		t.Fatal(err)
	}
	t.Log(uuid)
}
