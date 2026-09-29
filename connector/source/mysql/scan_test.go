package mysql

import (
	"testing"

	"github.com/PyRSA/go-cdc/runtime/checkpoint"
)

func TestScanDoneStopsAtEndAndAfterRotate(t *testing.T) {
	end := checkpoint.Position{File: "mysql-bin.000009", Pos: 1000}
	if scanDone("mysql-bin.000009", 999, end) {
		t.Fatal("stopped before the end position")
	}
	if !scanDone("mysql-bin.000009", 1000, end) {
		t.Fatal("did not stop at the end position")
	}
	if scanDone("mysql-bin.000008", 5000, end) {
		t.Fatal("an earlier file must keep scanning")
	}
	if !scanDone("mysql-bin.000010", 4, end) {
		t.Fatal("a later file must stop the scan")
	}
	if binlogFileAfter("mysql-bin.2", "mysql-bin.10") {
		t.Fatal("numeric suffix 2 is not after 10")
	}
}
