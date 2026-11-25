package cache

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestPACCompressionPolicy(t *testing.T) {
	if (Policy{}).PAC.level() != 1 {
		t.Fatal("default must remain fast")
	}
	for _, level := range []int{-1, 0, 1, 6, 9, 22, 23} {
		raw := fmt.Sprintf("version=1\ntimezone='UTC'\n[defaults.pac]\nlevel=%d\n", level)
		c, err := ParseConfiguration([]byte(raw))
		if level < 1 || level > 22 {
			if err == nil {
				t.Fatalf("accepted level %d", level)
			}
		} else if err != nil || c.Resolve("payment/transactions.v1").PAC.level() != level {
			t.Fatalf("level %d: %v", level, err)
		}
	}
	c, err := ParseConfiguration([]byte("version=1\ntimezone='UTC'\n[defaults.pac]\nlevel=9\n[resources.'payment/transactions.v1'.pac]\nlevel=3\n"))
	if err != nil || c.Resolve("payment/transactions.v1").PAC.level() != 3 || c.Resolve("other/resource.v1").PAC.level() != 9 {
		t.Fatal("registry inheritance", err)
	}
	path := filepath.Join(t.TempDir(), "debug.conf")
	for _, inherit := range []bool{true, false} {
		if err := os.WriteFile(path, []byte(fmt.Sprintf("version=1\ninherit=%t\n", inherit)), 0600); err != nil {
			t.Fatal(err)
		}
		p, _, err := readScope(path, c.Defaults, true)
		want := 1
		if inherit {
			want = 9
		}
		if err != nil || p.PAC.level() != want {
			t.Fatal("scope inheritance", inherit, p, err)
		}
	}
}
