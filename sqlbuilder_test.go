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
		Or([]*Condition{
			WhereNotEmpty("t.e=?", "x"),
			WhereNotEmpty("t.d=?", "y"),
			WhereNotEmpty("t.f=?", []string{}),
		}).
		Join("table_x", "x", "x.tid=t.id").
		OrderBy("t.a desc").
		OrderBy("t.b desc").
		GroupBy("t.category asc").
		Limit(10).
		Offset(10).
		SelectSql()

	expectedSql := "SELECT a,b,c FROM testing t WHERE t.a=$1 AND t.b=$2 AND (t.e=$3 OR t.d=$4) JOIN table_x x ON x.tid=t.id GROUP BY t.category asc ORDER BY t.a desc,t.b desc LIMIT 10 OFFSET 10"
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

func TestBuilder_Insert(t *testing.T) {
	builder := NewBuilder("testing", "t")

	p := builder.
		InsertValue("a", 321).
		InsertValue("b", "whatsup").
		InsertSql()

	expectedSql := "INSERT INTO testing t (a,b) VALUES ($1,$2)"
	if p.Sql != expectedSql {
		t.Logf("generated sql not matched:\n\texpected: %s\n\t   found: %s\n", expectedSql, p.Sql)
		t.Fail()
	}

	if v, ok := p.Args[0].(int); !ok || v != 321 {
		t.Logf("args 1 not matched:\n\texpected: %d\n\t   found: %v\n", 123, v)
		t.Fail()
	}
	if v, ok := p.Args[1].(string); !ok || v != "whatsup" {
		t.Logf("args 2 not matched:\n\texpected: %d\n\t   found: %v\n", 123, v)
		t.Fail()
	}
}

func TestBuilder_Update_1(t *testing.T) {
	builder := NewBuilder("testing", "t")

	p := builder.
		UpdateValue("a=?", 321).
		UpdateValue("b=?", "whatsup").
		Where("a != ?", 43).
		UpdateSql()

	expectedSql := "UPDATE testing t SET a=$1,b=$2 WHERE a != $3"
	if p.Sql != expectedSql {
		t.Logf("generated sql not matched:\n\texpected: %s\n\t   found: %s\n", expectedSql, p.Sql)
		t.Fail()
	}

	if v, ok := p.Args[0].(int); !ok || v != 321 {
		t.Logf("args 1 not matched:\n\texpected: %d\n\t   found: %v\n", 321, v)
		t.Fail()
	}
	if v, ok := p.Args[1].(string); !ok || v != "whatsup" {
		t.Logf("args 2 not matched:\n\texpected: %d\n\t   found: %v\n", 123, v)
		t.Fail()
	}
	if v, ok := p.Args[2].(int); !ok || v != 43 {
		t.Logf("args 1 not matched:\n\texpected: %d\n\t   found: %v\n", 43, v)
		t.Fail()
	}
}

func TestBuilder_Update_2(t *testing.T) {
	builder := NewBuilder("testing", "t")

	p := builder.
		Where("a != ?", 34).
		UpdateValue("a=?", 321).
		UpdateValue("b=?", "whatsup").
		UpdateSql()

	expectedSql := "UPDATE testing t SET a=$2,b=$3 WHERE a != $1"
	if p.Sql != expectedSql {
		t.Logf("generated sql not matched:\n\texpected: %s\n\t   found: %s\n", expectedSql, p.Sql)
		t.Fail()
	}

	if v, ok := p.Args[0].(int); !ok || v != 34 {
		t.Logf("args 1 not matched:\n\texpected: %d\n\t   found: %v\n", 34, v)
		t.Fail()
	}
	if v, ok := p.Args[1].(int); !ok || v != 321 {
		t.Logf("args 1 not matched:\n\texpected: %d\n\t   found: %v\n", 321, v)
		t.Fail()
	}
	if v, ok := p.Args[2].(string); !ok || v != "whatsup" {
		t.Logf("args 2 not matched:\n\texpected: %d\n\t   found: %v\n", 123, v)
		t.Fail()
	}
}
