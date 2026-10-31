package mysql

import "testing"

func TestUpdatePairsStayOneEvent(t *testing.T) {
	events, err := RowsToEvents("update", "tests", "users", []string{"id", "name"},
		[]string{"bigint", "varchar(64)"}, [][]any{
			{int32(1), "alice"},
			{int32(1), "alice2"},
			{int32(2), "bob"},
			{int32(2), "bobby"},
		})
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 {
		t.Fatalf("got %d events", len(events))
	}
	if events[0].Op != "u" || events[0].Before["name"] != "alice" || events[0].After["name"] != "alice2" {
		t.Fatalf("%#v", events[0])
	}
	if events[0].Before["id"] != int64(1) {
		t.Fatalf("id type %#v", events[0].Before["id"])
	}
}

func TestOddUpdateRejected(t *testing.T) {
	_, err := RowsToEvents("update", "tests", "users", []string{"id"}, []string{"bigint"},
		[][]any{{int32(1)}})
	if err == nil {
		t.Fatal("expected error")
	}
}

// TestBinaryColumnsMatchTheSnapshotShape pins the one place where the two capture
// paths used to disagree: go-mysql decodes BINARY(n) and VARBINARY(n) into a Go
// string, while the snapshot returns a byte slice, and a string argument reaches a
// PostgreSQL bytea column as text, where byteain() stops at the first NUL byte.
//
// The inputs below are the shapes go-mysql actually produces (v1.16.0
// replication/row_event.go::decodeValue): BINARY(n)/VARBINARY(n) as Go string,
// BLOB and TEXT as []byte, VARCHAR as Go string.
func TestBinaryColumnsMatchTheSnapshotShape(t *testing.T) {
	columns := []string{"id", "c_bin", "c_vbin", "c_blob", "c_text", "c_vc", "c_null"}
	types := []string{"bigint", "binary(4)", "varbinary(16)", "blob", "text", "varchar(64)",
		"binary(4)"}
	events, err := RowsToEvents("insert", "tests", "typed", columns, types, [][]any{
		{int64(1), "\xff\xff\xff\xff", "\x00", "\x00\xff", []byte("keep"), "plain", nil},
		{int64(2), "", "\x00\x01", "", []byte{}, "", nil},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 {
		t.Fatalf("got %d events", len(events))
	}

	first := events[0].After
	for _, column := range []string{"c_bin", "c_vbin", "c_blob"} {
		if _, ok := first[column].([]byte); !ok {
			t.Fatalf("%s type %T, want []byte", column, first[column])
		}
	}
	if got := first["c_bin"].([]byte); string(got) != "\xff\xff\xff\xff" {
		t.Fatalf("c_bin %x", got)
	}
	if got := first["c_vbin"].([]byte); len(got) != 1 || got[0] != 0x00 {
		t.Fatalf("c_vbin %x", got)
	}
	// TEXT arrives as []byte and must stay one: only the *binary* types are
	// restored, a text column is never rewritten.
	text, ok := first["c_text"].([]byte)
	if !ok || string(text) != "keep" {
		t.Fatalf("c_text %#v, want the decoded []byte", first["c_text"])
	}
	// VARCHAR arrives as a Go string and must stay one.
	if got := first["c_vc"]; got != "plain" {
		t.Fatalf("c_vc %#v, want the decoded string", got)
	}
	if got := first["c_null"]; got != nil {
		t.Fatalf("c_null %#v, want nil", got)
	}

	// An empty value is still a value, not NULL, on both shapes.
	second := events[1].After
	empty, ok := second["c_bin"].([]byte)
	if !ok || empty == nil || len(empty) != 0 {
		t.Fatalf("c_bin %#v, want a non-nil empty slice", second["c_bin"])
	}
	emptyText, ok := second["c_text"].([]byte)
	if !ok || emptyText == nil || len(emptyText) != 0 {
		t.Fatalf("c_text %#v, want a non-nil empty slice", second["c_text"])
	}
}

// TestRowsToEventsWithoutTypesKeepsTheDecodedValue covers callers with no
// catalogue: the binary shape cannot be recovered, so the value is left alone
// rather than guessed.
func TestRowsToEventsWithoutTypesKeepsTheDecodedValue(t *testing.T) {
	events, err := RowsToEvents("insert", "tests", "typed", []string{"c_bin"}, nil,
		[][]any{{"\xff"}})
	if err != nil {
		t.Fatal(err)
	}
	if got := events[0].After["c_bin"]; got != "\xff" {
		t.Fatalf("c_bin %#v", got)
	}
}
