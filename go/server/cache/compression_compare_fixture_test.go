package cache

import (
 "os"
 "path/filepath"
 "testing"
 "time"
 "encoding/binary"
)

// Opt-in codec comparison. No live store is opened or modified.
func TestExportCompressionComparisonPACs(t *testing.T) {
 out := os.Getenv("PAC_CODEC_FIXTURES")
 if out == "" { t.Skip("set PAC_CODEC_FIXTURES to export isolated benchmark PACs") }
 for _, sparse := range []bool{false,true} {
  name := "dense"; if sparse { name = "sparse" }
  l, frames := pacWorkload(t,10000,sparse)
  at:=time.UnixMilli(1790000000000)
  store,err:=Open(Config{Root:t.TempDir(),Cache:"bench",Tenant:"test",Layout:l,Now:func()time.Time{return at.Add(72*time.Hour)}})
  if err!=nil{t.Fatal(err)}
  off:=false;store.policy.SearchIndex=&off
  if _,err=store.AppendBatch(Read,frames);err!=nil{t.Fatal(err)}
  if err=store.Flush();err!=nil{t.Fatal(err)}
  if err=store.Close();err!=nil{t.Fatal(err)}
  var packs []byte
  err=filepath.WalkDir(store.config.Root,func(path string,d os.DirEntry,e error)error{if e!=nil{return e};if d.Name()=="read.packs"{packs,e=os.ReadFile(path)};return e})
  if err!=nil{t.Fatal(err)}
  if len(packs)<=headerSize{t.Fatal("missing sealed PAC")}
  size:=int(binary.LittleEndian.Uint32(packs[headerSize:]))
  if headerSize+size!=len(packs){t.Fatal("expected one sealed hour PAC")}
  raw,err:=expandPAC(packs[headerSize:]);if err!=nil{t.Fatal(err)}
  if err=os.MkdirAll(filepath.Join(out,name),0755);err!=nil{t.Fatal(err)}
  if err=os.WriteFile(filepath.Join(out,name,"10000.pac"),raw,0644);err!=nil{t.Fatal(err)}
  t.Log(name,len(raw),"bytes",binary.LittleEndian.Uint32(raw[12:16]),"records")
 }
}
