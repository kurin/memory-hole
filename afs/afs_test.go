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
	if err := writeFile(fs, "/file/a", "loopy\n"); err != nil {
		t.Fatal(err)
	}
	if err := writeFile(fs, "/file/b", "loopier\n"); err != nil {
		t.Fatal(err)
	}
	uuid, err := fs.Finalize()
	if err != nil {
		t.Fatal(err)
	}
	t.Log(uuid)
}

func writeFile(fs *FileSystem, name, body string) error {
	f, err := fs.Open(name)
	if err != nil {
		return err
	}
	buf := &bytes.Buffer{}
	buf.WriteString(body)
	if _, err := io.Copy(f, buf); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}
