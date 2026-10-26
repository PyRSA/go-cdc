package elasticsearch

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
)

const (
	maxIndexNameBytes = 255
	indexHashBytes    = 6 // 12 hex characters
)

// indexNameFor returns the Elasticsearch index for a routed table. An explicit
// sink.index-name wins for every table; otherwise the routed database.table is
// normalized so Elasticsearch accepts it.
func indexNameFor(explicit, database, table string) string {
	if explicit != "" {
		return explicit
	}
	return normalizeIndexName(database + "." + table)
}

// normalizeIndexName lowercases the routed name, replaces characters that
// Elasticsearch rejects, and appends a short hash whenever normalization
// changed the name. The hash keeps case-different tables from collapsing into
// one index.
func normalizeIndexName(raw string) string {
	cleaned := make([]byte, 0, len(raw))
	for i := 0; i < len(raw); i++ {
		c := raw[i]
		switch {
		case c >= 'A' && c <= 'Z':
			cleaned = append(cleaned, c-'A'+'a')
		case c >= 'a' && c <= 'z', c >= '0' && c <= '9', c == '.', c == '_', c == '-':
			cleaned = append(cleaned, c)
		default:
			cleaned = append(cleaned, '_')
		}
	}
	name := string(cleaned)
	if name == "" {
		name = "cdc"
	}
	if name[0] == '_' || name[0] == '-' || name[0] == '+' {
		name = "cdc" + name
	}
	if name != raw && raw != "" {
		name += "-" + shortHash(raw)
	}
	if len(name) > maxIndexNameBytes {
		suffix := "-" + shortHash(raw)
		name = name[:maxIndexNameBytes-len(suffix)] + suffix
	}
	return name
}

func shortHash(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:indexHashBytes])
}

// createPlan is the Create Index body plus the effective properties used for
// compatibility checks. Dynamic is the root mapping setting from the file (""
// when the file does not set one).
type createPlan struct {
	Body    json.RawMessage
	Props   map[string]json.RawMessage
	Dynamic string
}

// buildCreatePlan merges the optional mapping file with the derived mapping.
// The file wins per field; missing fields are derived from the final Schema.
func buildCreatePlan(fileBody []byte, derived map[string]json.RawMessage) (createPlan, error) {
	top := map[string]json.RawMessage{}
	if len(fileBody) > 0 {
		if err := json.Unmarshal(fileBody, &top); err != nil {
			return createPlan{}, fmt.Errorf("mapping-file: must be a JSON object: %w", err)
		}
	}
	if _, ok := top["mapping"]; ok {
		return createPlan{}, fmt.Errorf(`mapping-file: use "mappings" (Create Index key), not "mapping"`)
	}
	if _, ok := top["properties"]; ok {
		return createPlan{}, fmt.Errorf(`mapping-file: "properties" must be nested under "mappings"`)
	}
	mappings := map[string]json.RawMessage{}
	if raw, ok := top["mappings"]; ok {
		if err := json.Unmarshal(raw, &mappings); err != nil {
			return createPlan{}, fmt.Errorf(`mapping-file: "mappings" must be an object`)
		}
		if _, nested := mappings["mappings"]; nested {
			return createPlan{}, fmt.Errorf(`mapping-file: "mappings" must not be nested twice`)
		}
	}
	props := map[string]json.RawMessage{}
	if raw, ok := mappings["properties"]; ok {
		if err := json.Unmarshal(raw, &props); err != nil {
			return createPlan{}, fmt.Errorf(`mapping-file: "mappings.properties" must be an object`)
		}
	}
	rootDynamic := ""
	if raw, ok := mappings["dynamic"]; ok {
		var value any
		if err := json.Unmarshal(raw, &value); err != nil {
			return createPlan{}, fmt.Errorf(`mapping-file: "mappings.dynamic" is invalid`)
		}
		switch value.(type) {
		case string, bool:
			rootDynamic = dynamicName(value)
		default:
			return createPlan{}, fmt.Errorf(`mapping-file: "mappings.dynamic" must be a string or boolean`)
		}
	}
	for name, field := range derived {
		if _, ok := props[name]; !ok {
			props[name] = field
		}
	}
	propsRaw, err := json.Marshal(props)
	if err != nil {
		return createPlan{}, fmt.Errorf("mapping-file: encode properties: %w", err)
	}
	mappings["properties"] = propsRaw
	mappingsRaw, err := json.Marshal(mappings)
	if err != nil {
		return createPlan{}, fmt.Errorf("mapping-file: encode mappings: %w", err)
	}
	top["mappings"] = mappingsRaw
	body, err := json.Marshal(top)
	if err != nil {
		return createPlan{}, fmt.Errorf("mapping-file: encode body: %w", err)
	}
	return createPlan{Body: body, Props: props, Dynamic: rootDynamic}, nil
}

// propertiesFromMappings extracts properties and the root dynamic setting from
// a Get Mapping response.
func propertiesFromMappings(mappings map[string]any) (map[string]json.RawMessage, string, error) {
	rootDynamic := dynamicName(mappings["dynamic"])
	rawProperties, ok := mappings["properties"]
	if !ok || rawProperties == nil {
		return map[string]json.RawMessage{}, rootDynamic, nil
	}
	encoded, err := json.Marshal(rawProperties)
	if err != nil {
		return nil, "", fmt.Errorf("invalid properties: %w", err)
	}
	props := map[string]json.RawMessage{}
	if err := json.Unmarshal(encoded, &props); err != nil {
		return nil, "", fmt.Errorf(`"properties" must be an object`)
	}
	return props, rootDynamic, nil
}

func dynamicName(value any) string {
	switch typed := value.(type) {
	case string:
		return strings.ToLower(typed)
	case bool:
		if typed {
			return "true"
		}
		return "false"
	default:
		return ""
	}
}
