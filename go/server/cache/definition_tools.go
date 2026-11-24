package cache

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
)

// InstallDefinitionTools installs the existing tools; it never builds or changes
// a codec. A packaged server keeps doc and rx beside its executable. Development
// invocations may resolve installed tools on PATH instead.
func InstallDefinitionTools(root, source string) error {
	dir := filepath.Join(root, "definitions")
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	for _, name := range []string{"doc", "rx"} {
		candidate := filepath.Join(source, name)
		info, err := os.Stat(candidate)
		if err != nil || !info.Mode().IsRegular() {
			candidate, err = exec.LookPath(name)
			if err != nil {
				return fmt.Errorf("locate %s for definitions: %w", name, err)
			}
		}
		destination := filepath.Join(dir, name)
		a, _ := filepath.Abs(candidate)
		b, _ := filepath.Abs(destination)
		if a == b {
			continue
		}
		if err = copyDefinitionTool(candidate, destination); err != nil {
			return err
		}
	}
	return nil
}
func copyDefinitionTool(source, destination string) error {
	in, err := os.Open(source)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.CreateTemp(filepath.Dir(destination), ".tool-*")
	if err != nil {
		return err
	}
	defer os.Remove(out.Name())
	if _, err = io.Copy(out, in); err == nil {
		err = out.Chmod(0755)
	}
	if err == nil {
		err = out.Sync()
	}
	closeErr := out.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	return os.Rename(out.Name(), destination)
}
func initializeTrunkConfig(root string) error {
	f, err := os.OpenFile(configurationPath(root, nil), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if os.IsExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	_, err = f.WriteString("version = 1\n")
	if err == nil {
		err = f.Sync()
	}
	e := f.Close()
	if err != nil {
		return err
	}
	return e
}
