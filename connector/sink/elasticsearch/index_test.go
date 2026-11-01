package elasticsearch

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestIndexNameForUsesRoutedTable(t *testing.T) {
	got := indexNameFor("", "shop", "orders")
	if got != "shop.orders" {
		t.Fatalf("indexNameFor = %q, want shop.orders", got)
	}
	if strings.Contains(got, "default") {
		t.Fatalf("index name %q must not add a namespace prefix", got)
	}
}

func TestIndexNameForExplicitWins(t *testing.T) {
	if got := indexNameFor("custom_index", "shop", "orders"); got != "custom_index" {
		t.Fatalf("indexNameFor = %q, want custom_index", got)
	}
}

func TestNormalizeIndexNameIsStableAndUnique(t *testing.T) {
	first := normalizeIndexName("Shop.Orders")
	second := normalizeIndexName("Shop.Orders")
	if first != second {
		t.Fatalf("normalization is not stable: %q != %q", first, second)
	}
	if first != "shop.orders-"+shortHash("Shop.Orders") {
		t.Fatalf("normalizeIndexName = %q, want lowercased name plus hash", first)
	}
	plain := normalizeIndexName("shop.orders")
	if plain != "shop.orders" {
		t.Fatalf("normalizeIndexName = %q, want unchanged name", plain)
	}
	if plain == first {
		t.Fatal("case-different tables collapsed into one index")
	}
}

func TestNormalizeIndexNameRewritesIllegalCharacters(t *testing.T) {
	cases := map[string]string{
		"order items": "order_items-" + shortHash("order items"),
		"_hidden":     "cdc_hidden-" + shortHash("_hidden"),
		"-dash":       "cdc-dash-" + shortHash("-dash"),
		"a*b?c":       "a_b_c-" + shortHash("a*b?c"),
		"таблица":     "cdc" + strings.Repeat("_", len("таблица")) + "-" + shortHash("таблица"),
		"":            "cdc",
	}
	for raw, want := range cases {
		if got := normalizeIndexName(raw); got != want {
			t.Errorf("normalizeIndexName(%q) = %q, want %q", raw, got, want)
		}
	}
}

func TestNormalizeIndexNameTruncates(t *testing.T) {
	long := strings.Repeat("a", 300)
	got := normalizeIndexName(long)
	if len(got) != maxIndexNameBytes {
		t.Fatalf("len(%q) = %d, want %d", got, len(got), maxIndexNameBytes)
	}
	if !strings.HasSuffix(got, shortHash(long)) {
		t.Fatalf("truncated name %q lost its anti-collision hash", got)
	}
}

func TestBuildCreatePlanKeepsUserSettings(t *testing.T) {
	file := []byte(`{"settings":{"number_of_shards":1},"aliases":{"orders":{"is_write_index":true}},` +
		`"mappings":{"dynamic":"strict","properties":{"id":{"type":"keyword"}}}}`)
	derived := map[string]json.RawMessage{
		"id":   json.RawMessage(`{"type":"long"}`),
		"name": json.RawMessage(`{"type":"keyword"}`),
	}
	plan, err := buildCreatePlan(file, derived)
	if err != nil {
		t.Fatalf("buildCreatePlan: %v", err)
	}
	if plan.Dynamic != "strict" {
		t.Fatalf("Dynamic = %q, want strict", plan.Dynamic)
	}
	if got := string(plan.Props["id"]); got != `{"type":"keyword"}` {
		t.Fatalf("file mapping must win, got %s", got)
	}
	if _, ok := plan.Props["name"]; !ok {
		t.Fatal("derived property for a column missing from the file was dropped")
	}
	var body map[string]json.RawMessage
	if err := json.Unmarshal(plan.Body, &body); err != nil {
		t.Fatalf("body is not JSON: %v", err)
	}
	for _, key := range []string{"settings", "aliases", "mappings"} {
		if _, ok := body[key]; !ok {
			t.Fatalf("create body lost %q: %s", key, plan.Body)
		}
	}
}

func TestBuildCreatePlanRejectsWrongShape(t *testing.T) {
	cases := map[string]string{
		"singular mapping":  `{"mapping":{}}`,
		"top-level props":   `{"properties":{"id":{"type":"long"}}}`,
		"nested mappings":   `{"mappings":{"mappings":{}}}`,
		"invalid mappings":  `{"mappings":[]}`,
		"invalid props":     `{"mappings":{"properties":[]}}`,
		"invalid dynamic":   `{"mappings":{"dynamic":[]}}`,
		"not a JSON object": `[]`,
	}
	for name, body := range cases {
		if _, err := buildCreatePlan([]byte(body), map[string]json.RawMessage{}); err == nil {
			t.Errorf("%s: expected an error for %s", name, body)
		}
	}
}

func TestPropertiesFromMappings(t *testing.T) {
	props, dynamic, err := propertiesFromMappings(map[string]any{
		"dynamic": false,
		"properties": map[string]any{
			"id": map[string]any{"type": "long"},
		},
	})
	if err != nil {
		t.Fatalf("propertiesFromMappings: %v", err)
	}
	if dynamic != "false" {
		t.Fatalf("dynamic = %q, want false", dynamic)
	}
	if got := string(props["id"]); got != `{"type":"long"}` {
		t.Fatalf("props[id] = %s", got)
	}

	props, dynamic, err = propertiesFromMappings(map[string]any{"dynamic": "strict"})
	if err != nil {
		t.Fatalf("propertiesFromMappings: %v", err)
	}
	if dynamic != "strict" || len(props) != 0 {
		t.Fatalf("dynamic = %q, props = %v", dynamic, props)
	}
}
