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
	inserts   []string
	updates   map[string]string
	wheres    map[string]string
	ors       [][]*Condition
	joins     []*JoinInfo
	groups    []string
	orders    []string
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

func (b *Builder) Where(field string, values ...interface{}) *Builder {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.wheres[field]; ok {
		return b
	}

	s := field
	for _, value := range values {
		newValue := value
		b.args = append(b.args, newValue)
		s = strings.Replace(s, "?", fmt.Sprintf("$%d", len(b.args)), 1)
	}
	b.wheres[field] = s
	return b
}

func isEmpty(value interface{}) bool {
	if value == nil {
		return true
	}

	kind := reflect.TypeOf(value).Kind()
	switch kind {
	case reflect.Slice, reflect.Array, reflect.Map:
		if reflect.ValueOf(value).Len() == 0 {
			return true
		}
	}

	switch v := value.(type) {
	case string:
		if v == "" {
			return true
		}
	case int, int32, int64:
		if v == 0 {
			return true
		}
	case float32, float64:
		if v == 0.0 {
			return true
		}
	case time.Time:
		if v.UnixMicro() == 0 {
			return true
		}
	}
	return false
}

type Condition struct {
	field string
	value interface{}
}

func WhereNotEmpty(field string, value interface{}) *Condition {
	if isEmpty(value) {
		return nil
	}
	return &Condition{field, value}
}

func (b *Builder) WhereNotEmpty(field string, value interface{}) *Builder {
	if isEmpty(value) {
		return b
	}
	return b.Where(field, value)
}

func (b *Builder) Or(conditions []*Condition) *Builder {
	var filtered []*Condition
	for _, cond := range conditions {
		if cond != nil {
			filtered = append(filtered, cond)
		}
	}

	if len(filtered) == 0 {
		return b
	}

	var ors []string
	var args []interface{}
	for _, cond := range filtered {
		ors = append(ors, cond.field)
		args = append(args, cond.value)
	}

	orSql := fmt.Sprintf("(%s)", strings.Join(ors, " OR "))
	b.Where(orSql, args...)

	return b
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

func (b *Builder) OrderBy(stmt string) *Builder {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.orders = append(b.orders, stmt)
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

func (b *Builder) buildInsertArgSql() *WhereSqlPrepared {
	if len(b.args) == 0 {
		return nil
	}

	var statements []string
	for i := range b.args {
		statements = append(statements, fmt.Sprintf(`$%d`, i+1))
	}

	sql := fmt.Sprintf(`%s`, strings.Join(statements, ","))

	return &WhereSqlPrepared{
		Sql: "(" + sql + ")",
	}
}

func (b *Builder) buildUpdateArgSql() *UpdateSqlPrepared {
	if len(b.updates) == 0 {
		return nil
	}

	var statements []string
	for _, s := range b.updates {
		statements = append(statements, s)
	}

	sql := fmt.Sprintf(`SET %s`, strings.Join(statements, ","))

	return &UpdateSqlPrepared{
		Sql: sql,
	}
}

func (b *Builder) orderSql() string {
	if len(b.orders) == 0 {
		return ""
	}
	return fmt.Sprintf("ORDER BY %s", strings.Join(b.orders, ","))
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
	orderSql := b.orderSql()
	limitSql := b.limitSql()
	offsetSql := b.offsetSql()
	sql := fmt.Sprintf(`SELECT %s FROM %s %s %s %s %s %s %s %s`,
		selectSql,
		b.tableName, b.tableAlias,
		wherePrepared.Sql,
		joinSql,
		groupSql,
		orderSql,
		limitSql, offsetSql)

	countSql := fmt.Sprintf("SELECT count(*) FROM %s %s %s %s", b.tableName, b.tableAlias, wherePrepared.Sql, groupSql)

	return &SelectSqlPrepared{
		Sql:      sql,
		CountSql: countSql,
		Args:     b.args,
	}
}

type InsertSqlPrepared struct {
	Sql  string
	Args []interface{}
}

func (b *Builder) InsertSql() *InsertSqlPrepared {
	var fields []string
	for _, name := range b.inserts {
		fields = append(fields, name)
	}
	fieldSql := strings.Join(fields, ",")
	argSql := b.buildInsertArgSql()

	sql := fmt.Sprintf(`INSERT INTO %s %s (%s) VALUES %s`,
		b.tableName, b.tableAlias, fieldSql, argSql.Sql)

	return &InsertSqlPrepared{
		Sql:  sql,
		Args: b.args,
	}
}

func (b *Builder) UpdateSql() *UpdateSqlPrepared {
	whereSql := b.buildWhereSql()
	argSql := b.buildUpdateArgSql()

	sql := fmt.Sprintf(`UPDATE %s %s %s %s`,
		b.tableName, b.tableAlias, argSql.Sql, whereSql.Sql)

	return &UpdateSqlPrepared{
		Sql:  sql,
		Args: b.args,
	}
}

func (b *Builder) InsertValue(n string, v interface{}) *Builder {
	b.args = append(b.args, v)
	b.inserts = append(b.inserts, n)
	return b
}

type UpdateSqlPrepared struct {
	Sql  string
	Args []interface{}
}

func (b *Builder) UpdateValue(field string, value interface{}) *Builder {
	b.mu.Lock()
	defer b.mu.Unlock()

	newValue := value

	if _, ok := b.updates[field]; ok {
		return b
	}
	b.args = append(b.args, newValue)
	s := strings.Replace(field, "?", fmt.Sprintf("$%d", len(b.args)), -1)
	b.updates[field] = s
	return b
}

func (b *Builder) UpdateValueNotEmpty(field string, value interface{}) *Builder {
	if isEmpty(b) {
		return b
	}
	return b.UpdateValue(field, value)
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
		updates:    map[string]string{},
		mu:         &sync.Mutex{},
	}
}
