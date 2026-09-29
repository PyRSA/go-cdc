package mysql

import (
	"testing"

	"github.com/PyRSA/go-cdc/common/schema"
)

func TestCreateNullabilityFollowsTheStatement(t *testing.T) {
	table := schema.Table{Columns: []schema.Column{
		{Name: "id", Type: "int", Nullable: true},
		{Name: "name", Type: "varchar(32)", Nullable: true},
		{Name: "city", Type: "varchar(16)", Nullable: false},
	}}
	ddl := "CREATE TABLE db.t (id int NOT NULL, name varchar(32) NOT NULL, city varchar(16) NULL, PRIMARY KEY (id))"
	applyCreateNullability(&table, ddl)
	if table.Columns[0].Nullable || table.Columns[1].Nullable || !table.Columns[2].Nullable {
		t.Fatalf("%#v", table.Columns)
	}
}
