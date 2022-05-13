package gosimplesqlbuilder

import (
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"
)

type SelectField struct {
	Name  string
	Value interface{}
}

type QueryType string

const (
	QueryTypeSelect = "select"
	QueryTypeInsert = "insert"
	QueryTypeUpdate = "update"
	QueryTypeDelete = "delete"
)

type JoinInfo struct {
	TableName, TableAlias, Condition string
}

type Builder struct {
	tableName  string
	tableAlias string

	selects   []string
	wheres    map[string]string
	joins     []*JoinInfo
	groups    []string
	queryType QueryType
	args      []interface{}

	limit  int
	offset int

	mu *sync.Mutex
}

func (b *Builder) Select(fields ...string) *Builder {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.queryType = QueryTypeSelect
	b.selects = append(b.selects, fields...)
	return b
}

func (b *Builder) Where(field string, value interface{}) *Builder {
	b.mu.Lock()
	defer b.mu.Unlock()

	newValue := value

	if _, ok := b.wheres[field]; ok {
		return b
	}
	b.args = append(b.args, newValue)
	s := strings.Replace(field, "?", fmt.Sprintf("$%d", len(b.args)), -1)
	b.wheres[field] = s
	return b
}

func (b *Builder) WhereNotEmpty(field string, value interface{}) *Builder {
	if value == nil {
		return b
	}

	kind := reflect.TypeOf(value).Kind()
	switch kind {
	case reflect.Slice, reflect.Array, reflect.Map:
		if reflect.ValueOf(value).Len() == 0 {
			return b
		}
	}

	switch v := value.(type) {
	case string:
		if v == "" {
			return b
		}
	case int, int32, int64:
		if v == 0 {
			return b
		}
	case float32, float64:
		if v == 0.0 {
			return b
		}
	case time.Time:
		if v.UnixMicro() == 0 {
			return b
		}
	}

	return b.Where(field, value)
}

func (b *Builder) Join(name, alias, condition string) *Builder {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.joins = append(b.joins, &JoinInfo{
		TableName:  name,
		TableAlias: alias,
		Condition:  condition,
	})
	return b
}

func (b *Builder) GroupBy(stmt string) *Builder {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.groups = append(b.groups, stmt)

	return b
}

func (b *Builder) Limit(limit int) *Builder {
	if limit == 0 {
		return b
	}
	b.limit = limit
	return b
}

func (b *Builder) Offset(offset int) *Builder {
	if offset == 0 {
		return b
	}
	b.offset = offset
	return b
}

type SelectSqlPrepared struct {
	Sql        string
	CountSql   string
	Args       []interface{}
	ScanValues []interface{}
}

type WhereSqlPrepared struct {
	Sql  string
	Args []interface{}
}

type CountSqlPrepared struct {
	Sql  string
	Args []interface{}
}

type LimitPrepared struct {
	Sql  string
	Args []interface{}
}

type OffsetPrepared struct {
	Sql  string
	Args []interface{}
}

func (b *Builder) joinSql() string {
	var joins []string
	for _, it := range b.joins {
		joins = append(joins, fmt.Sprintf("JOIN %s %s ON %s", it.TableName, it.TableAlias, it.Condition))
	}

	return strings.Join(joins, " ")
}

func (b *Builder) buildWhereSql() *WhereSqlPrepared {
	if len(b.wheres) == 0 {
		return nil
	}

	var statements []string
	for _, s := range b.wheres {
		statements = append(statements, s)
	}

	sql := fmt.Sprintf(`WHERE %s`, strings.Join(statements, " AND "))

	return &WhereSqlPrepared{
		Sql: sql,
	}
}

func (b *Builder) groupSql() string {
	if len(b.groups) == 0 {
		return ""
	}
	return fmt.Sprintf("GROUP BY %s", strings.Join(b.groups, ","))
}

func (b *Builder) limitSql() string {
	if b.limit == 0 {
		return ""
	}
	return fmt.Sprintf("LIMIT %d", b.limit)
}

func (b *Builder) offsetSql() string {
	if b.offset == 0 {
		return ""
	}
	return fmt.Sprintf("OFFSET %d", b.offset)
}

func (b *Builder) SelectSql() *SelectSqlPrepared {
	var fields []string
	for _, name := range b.selects {
		fields = append(fields, name)
	}

	selectSql := strings.Join(fields, ",")
	wherePrepared := b.buildWhereSql()
	joinSql := b.joinSql()
	groupSql := b.groupSql()
	limitSql := b.limitSql()
	offsetSql := b.offsetSql()
	sql := fmt.Sprintf(`SELECT %s FROM %s %s %s %s %s %s %s`,
		selectSql,
		b.tableName, b.tableAlias,
		wherePrepared.Sql,
		joinSql,
		groupSql,
		limitSql, offsetSql)

	countSql := fmt.Sprintf("SELECT count(*) FROM %s %s %s %s", b.tableName, b.tableAlias, wherePrepared.Sql, groupSql)

	return &SelectSqlPrepared{
		Sql:      sql,
		CountSql: countSql,
		Args:     b.args,
	}
}

func NewBuilder(tableName, tableAlias string) *Builder {
	if tableAlias == "" {
		tableAlias = tableName
	}
	return &Builder{
		tableName:  tableName,
		tableAlias: tableAlias,
		selects:    []string{},
		wheres:     map[string]string{},
		mu:         &sync.Mutex{},
	}
}
