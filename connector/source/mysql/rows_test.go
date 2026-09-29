package mysql

import "testing"

func TestUpdatePairsStayOneEvent(t *testing.T) {
	events, err := RowsToEvents("update", "tests", "users", []string{"id", "name"}, [][]any{
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
	_, err := RowsToEvents("update", "tests", "users", []string{"id"}, [][]any{{int32(1)}})
	if err == nil {
		t.Fatal("expected error")
	}
}
