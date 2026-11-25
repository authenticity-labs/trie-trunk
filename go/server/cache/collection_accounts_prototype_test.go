//go:build trunk_prototype

package cache

import (
	"bufio"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	pill "github.com/authenticity-labs/pill-packer/go"
)

// Read the viewer's physical types from Rust and retain the authored RX annotations.
func fixtureFields(t *testing.T, definition *pill.Definition, rx []byte) []map[string]any {
	t.Helper()
	raw, err := definition.RuntimeLayout()
	if err != nil {
		t.Fatal(err)
	}
	var layout struct {
		Fields []map[string]any `pil:"fields"`
	}
	if err = pill.UnmarshalObject(raw, &layout); err != nil {
		t.Fatal(err)
	}
	var docs []struct {
		Fields []struct {
			Name        string
			Annotations []string
		}
	}
	if err = json.Unmarshal(rx, &docs); err != nil {
		t.Fatal(err)
	}
	if len(docs) != 1 || len(docs[0].Fields) != len(layout.Fields) {
		t.Fatal("unexpected RX layout")
	}
	for i, field := range docs[0].Fields {
		if field.Name != layout.Fields[i]["name"] {
			t.Fatal("unexpected RX field", field.Name)
		}
		if layout.Fields[i]["values"] == nil {
			layout.Fields[i]["values"] = []string{}
		}
		layout.Fields[i]["annotations"] = append([]string{}, field.Annotations...)
	}
	return layout.Fields
}

// These are the same rows used by the Snoot Collections preview. Resolve the
// revision postings against reopened uncompressed PACs before exporting a view.
func TestObjectIndexAccountsFixture(t *testing.T) {
	dir := "../../../testdata/collections-accounts"
	read := func(name string) []map[string]any {
		file, err := os.Open(filepath.Join(dir, name))
		if err != nil {
			t.Fatal(err)
		}
		defer file.Close()
		var rows []map[string]any
		scan := bufio.NewScanner(file)
		for scan.Scan() {
			var row map[string]any
			if err = json.Unmarshal(scan.Bytes(), &row); err != nil {
				t.Fatal(err)
			}
			rows = append(rows, row)
		}
		if err = scan.Err(); err != nil {
			t.Fatal(err)
		}
		return rows
	}
	accounts, logs := read("account.jsonl"), read("log-http.jsonl")
	defBytes, err := os.ReadFile(filepath.Join(dir, "account.rx.json"))
	if err != nil {
		t.Fatal(err)
	}
	def, err := pill.FromJSON(defBytes)
	if err != nil {
		t.Fatal(err)
	}
	defer def.Close()
	box, err := pill.NewBox(pill.BoxOptions{External: true})
	if err != nil {
		t.Fatal(err)
	}
	defer box.Close()
	index := testCollectionIndex(t)
	source := objectBinding{}
	origin := int64(accounts[0]["time"].(float64)) / TimeHourMS
	packs := map[uint32][][]byte{}
	var hours []uint32
	expected := map[string][]map[string]any{}
	var posts []collectionPosting
	for _, row := range accounts {
		id := row["id"].(string)
		rawID, err := hex.DecodeString(strings.ReplaceAll(id, "-", ""))
		if err != nil || len(rawID) != 16 {
			t.Fatal("invalid object UUID", id)
		}
		var octets [16]int
		for i, b := range rawID {
			octets[i] = int(b)
		}
		row["id"] = octets
		input, err := json.Marshal(row)
		if err != nil {
			t.Fatal(err)
		}
		row["id"] = id
		record, err := def.EncodeJSON(input)
		if err != nil {
			t.Fatal(err)
		}
		hour := uint32(int64(row["time"].(float64))/TimeHourMS - origin)
		if _, ok := packs[hour]; !ok {
			hours = append(hours, hour)
		}
		packs[hour] = append(packs[hour], record)
		posts = append(posts, collectionPosting{[16]byte(rawID), uint64(row["revision_id"].(float64)), collectionAddress{hour, uint32(len(packs[hour]))}})
		expected[id] = append(expected[id], row)
	}
	offsets := map[uint32]uint64{}
	var end uint64
	for _, hour := range hours {
		raw, err := def.Pack(packs[hour], nil)
		if err != nil {
			t.Fatal(err)
		}
		offsets[hour] = end
		end += uint64(len(raw))
		if err = box.Push(raw); err != nil {
			t.Fatal(err)
		}
	}
	raw, err := box.Seal()
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "account.pods")
	if err = os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	if err = index.append(source, 4, 0, uint64(len(posts)), posts); err != nil {
		t.Fatal(err)
	}
	index, err = loadObjectIndex(index.file)
	if err != nil {
		t.Fatal(err)
	}
	raw, err = os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	stored, err := pill.OpenBox(raw)
	if err != nil {
		t.Fatal(err)
	}
	defer stored.Close()
	histories := map[string][]map[string]any{}
	for id, want := range expected {
		key, _ := hex.DecodeString(strings.ReplaceAll(id, "-", ""))
		err = index.visit(key, source, func(revision uint64, addr collectionAddress) {
			pac, err := stored.At(offsets[addr.Hour])
			if err != nil {
				t.Fatal(err)
			}
			rows, _, err := def.Unpack(pac)
			if err != nil {
				t.Fatal(err)
			}
			if addr.Record == 0 || int(addr.Record) > len(rows) {
				t.Fatal("PIL ordinal", addr)
			}
			raw, err := def.DecodeJSON(rows[addr.Record-1])
			if err != nil {
				t.Fatal(err)
			}
			var record map[string]any
			if err = json.Unmarshal(raw, &record); err != nil {
				t.Fatal(err)
			}
			uuid, ok := record["id"].([]any)
			if !ok || len(uuid) != 16 {
				t.Fatal("unexpected UUID", record)
			}
			for i, b := range uuid {
				if b != float64(key[i]) {
					t.Fatal("wrong object", id)
				}
			}
			if record["revision_id"] != float64(revision) {
				t.Fatal("wrong revision", revision, record)
			}
			record["id"] = id
			histories[id] = append(histories[id], record)
		})
		if err != nil || !reflect.DeepEqual(histories[id], want) {
			t.Fatal(id, err, histories[id], want)
		}
	}
	seen := map[string]bool{}
	for _, log := range logs {
		event := log["id"].(string)
		if seen[event] || event == log["object_id"] {
			t.Fatal("event identity", event)
		}
		seen[event] = true
		id, _ := log["object_id"].(string)
		if id == "" || log["status"] == float64(404) {
			if len(histories[id]) != 0 {
				t.Fatal("unexpected history", id)
			}
			continue
		}
		if len(histories[id]) == 0 {
			t.Fatal("dangling log", id)
		}
		if rev, ok := log["revision_id"].(float64); ok {
			key, _ := hex.DecodeString(strings.ReplaceAll(id, "-", ""))
			if _, found, err := index.lookup(key, source, uint64(rev)); err != nil || !found {
				t.Fatal(id, rev, err)
			}
		}
	}
	if output := os.Getenv("SNOOT_ACCOUNTS_FIXTURE"); output != "" {
		logRX, err := os.ReadFile(filepath.Join(dir, "log-http.rx.json"))
		if err != nil {
			t.Fatal(err)
		}
		logDef, err := pill.FromJSON(logRX)
		if err != nil {
			t.Fatal(err)
		}
		defer logDef.Close()
		result, err := json.Marshal(map[string]any{"histories": histories, "logs": logs,
			"accountFields": fixtureFields(t, def, defBytes), "logFields": fixtureFields(t, logDef, logRX)})
		if err != nil {
			t.Fatal(err)
		}
		if err = os.WriteFile(output, result, 0600); err != nil {
			t.Fatal(err)
		}
	}
}
