package logging

import (
	_ "embed"
	"encoding/binary"
	"fmt"
	pilcodec "github.com/authenticity-labs/pill-packer/go/codec"
	"trie-trunk/go/codec"
	Keys "trie-trunk/go/logger/client/keys"
	"trie-trunk/go/logger/pil"
	"trie-trunk/go/models"
	"trie-trunk/go/server/cache"
)

//go:embed definitions/http-log.rx
var httpLogDefinition []byte

// BuildLogDefinition binds one compiled CDDL Definition to a log resource.
func BuildLogDefinition(binding pil.Binding) ([]byte, error) {
	binding.Format = pil.StorageFormat
	binding.Strategy = "log"
	binding.Roles = cache.Roles{Event: "@id", Duration: "duration"}
	return cache.BindAuthoredDefinition(httpLogDefinition, binding)
}
func compileLogEntry(l *cache.Layout, d *codec.Dictionary) (*pilEntryPlan, error) {
	p := &pilEntryPlan{layout: l, dictionary: d, slots: map[string]int{}, scratch: make([]pil.Value, len(l.Widths))}
	for i, f := range l.Definition.Fields {
		p.slots[f.Name] = i
	}
	for _, name := range []string{"@id", "@time", "duration", "SeverityText", "SeverityNumber", "Body", "Resource", "Attributes", "@fields"} {
		if _, ok := p.slots[name]; !ok {
			return nil, fmt.Errorf("log Definition lacks %s", name)
		}
	}
	return p, nil
}
func (p *pilEntryPlan) logValues(e *Entry) ([]pil.Value, error) {
	label := func(key codec.Key, s codec.Symbol) (string, error) {
		if s.IsZero() {
			return "", nil
		}
		if text, ok := s.Raw().(string); ok {
			return text, nil
		}
		field := entrySchema[key]
		if text, ok := field.Enums[s.Id()]; ok {
			return text, nil
		}
		if p.dictionary != nil {
			if text, ok := p.dictionary.Resolve(field.Domain, s).(string); ok {
				return text, nil
			}
		}
		return "", fmt.Errorf("undefined %s reference %d", field.Name, s.Id())
	}
	if e.Time.IsZero() {
		return nil, fmt.Errorf("log requires @time")
	}
	if e.Level < TRACE || e.Level > ERROR {
		return nil, fmt.Errorf("invalid log severity")
	}
	values := p.scratch
	clear(values)
	put := func(name string, v pil.Value) { values[p.slots[name]] = v }
	binary.LittleEndian.PutUint64(p.numeric[:8], uint64(e.Time.UnixMilli()))
	binary.LittleEndian.PutUint64(p.numeric[8:16], uint64(e.Duration.Microseconds()))
	p.numeric[16] = uint8(e.Level-1)*4 + 1
	put("@time", pil.Value{State: pil.Direct, Bytes: p.numeric[:8]})
	put("duration", pil.Value{State: pil.Direct, Bytes: p.numeric[8:16]})
	put("SeverityNumber", pil.Value{State: pil.Direct, Bytes: p.numeric[16:17]})
	put("SeverityText", cache.Text([]string{"", "TRACE", "DEBUG", "INFO", "WARN", "ERROR"}[e.Level]))
	object := map[string]any{}
	attrs := map[string]any{}
	resource := map[string]any{}
	for _, item := range []struct {
		key    codec.Key
		symbol codec.Symbol
		name   string
		target map[string]any
	}{
		{Keys.Service, e.Service, "service.name", resource}, {Keys.HttpMethod, e.HttpMethod, "http.request.method", attrs},
		{Keys.Message, e.Message, "Body", object}, {Keys.Operation, e.Operation, "EventName", object},
	} {
		text, err := label(item.key, item.symbol)
		if err != nil {
			return nil, err
		}
		if text != "" {
			item.target[item.name] = text
		}
	}
	if e.Version != "" {
		resource["service.version"] = e.Version
	}
	if e.Uri != "" {
		attrs["url.path"] = e.Uri
	}
	if e.ClientIp != "" {
		attrs["client.address"] = e.ClientIp
	}
	if e.present&(1<<Keys.HttpStatus) != 0 {
		attrs["http.response.status_code"] = e.HttpStatus
	}
	if e.present&(1<<Keys.ResponseBytes) != 0 {
		attrs["http.response.body.size"] = e.ResponseBytes
	}
	if e.Error != nil {
		attrs["exception"] = codec.Expand(e.Error.Record(), models.BuildSchema(), p.dictionary)
	}
	for name := range e.Fields {
		if _, owned := p.slots[name]; owned {
			return nil, fmt.Errorf("extension shadows logger field %s", name)
		}
		if _, owned := attrs[name]; owned {
			return nil, fmt.Errorf("extension shadows log attribute %s", name)
		}
	}
	if len(e.Fields) > 0 {
		// Ordinary open CDDL maps compile their additions into PIL's @fields.
		// Keep application additions separate from OpenTelemetry Attributes.
		raw, err := pilcodec.MarshalObject(e.Fields)
		if err != nil {
			return nil, err
		}
		put("@fields", pil.Value{State: pil.Offset, Bytes: raw})
	}
	if len(resource) > 0 {
		key := fmt.Sprint(resource["service.name"]) + "\x00" + e.Version
		if key != p.logResourceKey || p.logResource.State == cache.Absent {
			raw, err := pilcodec.MarshalObject(resource)
			if err != nil {
				return nil, err
			}
			p.logResource, err = p.layout.EncodeFieldObject(p.slots["Resource"], raw)
			if err != nil {
				return nil, err
			}
			p.logResourceKey = key
		}
		put("Resource", p.logResource)
	}
	if len(attrs) > 0 {
		raw, err := pilcodec.MarshalObject(attrs)
		if err != nil {
			return nil, err
		}
		value, err := p.layout.EncodeFieldObject(p.slots["Attributes"], raw)
		if err != nil {
			return nil, err
		}
		put("Attributes", value)
	}
	if body, ok := object["Body"]; ok {
		raw, err := pilcodec.MarshalObject(body)
		if err != nil {
			return nil, err
		}
		put("Body", pil.Value{State: pil.Offset, Bytes: raw})
	}
	if event, ok := object["EventName"].(string); ok {
		put("EventName", cache.Text(event))
	}
	return values, nil
}
