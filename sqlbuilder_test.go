package gosimplesqlbuilder

import (
	"testing"
)

func TestBuilder_Select(t *testing.T) {
	builder := NewBuilder("testing", "t")

	p := builder.
		Select("a").
		Select("b", "c").
		Where("t.a=?", 123).
		Where("t.b=?", "hello").
		WhereNotEmpty("t.c=?", nil).
		WhereNotEmpty("t.d=?", "").
		Join("table_x", "x", "x.tid=t.id").
		GroupBy("t.category asc").
		Limit(10).
		Offset(10).
		SelectSql()

	expectedSql := "SELECT a,b,c FROM testing t WHERE t.a=$1 AND t.b=$2 JOIN table_x x ON x.tid=t.id GROUP BY t.category asc LIMIT 10 OFFSET 10"
	if p.Sql != expectedSql {
		t.Logf("generated sql not matched:\n\texpected: %s\n\t   found: %s\n", expectedSql, p.Sql)
		t.Fail()
	}

	if v, ok := p.Args[0].(int); !ok || v != 123 {
		t.Logf("args 1 not matched:\n\texpected: %d\n\t   found: %v\n", 123, v)
		t.Fail()
	}
	if v, ok := p.Args[1].(string); !ok || v != "hello" {
		t.Logf("args 2 not matched:\n\texpected: %d\n\t   found: %v\n", 123, v)
		t.Fail()
	}
}