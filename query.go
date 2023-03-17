package litequery

import (
	"strings"
	"sync"
)

// Query is a struct that represents a query
type Query struct {
	query []byte
	args  []any
}

// Analyze is a function that returns an ANALYZE query
func Analyze(query string) *Query {
	return getQuery().Analyze(query)
}

// Analyze is a function that returns an ANALYZE query
func (q *Query) Analyze(query string) *Query {
	q.query = append(q.query, "ANALYZE "...)
	q.query = append(q.query, query...)
	q.query = append(q.query, ";"...)
	return q
}

// Explain is a function that returns an EXPLAIN query
func Explain(query string) *Query {
	return getQuery().Explain(query)
}

// Explain is a function that returns an EXPLAIN query
func (q *Query) Explain(query string) *Query {
	q.query = append(q.query, "EXPLAIN "...)
	q.query = append(q.query, query...)
	return q
}

// Begin is a function that returns a BEGIN TRANSACTION query with the specified mode
func Begin(mode string) *Query {
	return getQuery().Begin(mode)
}

// Begin is a function that returns a BEGIN TRANSACTION query with the specified mode
func (q *Query) Begin(mode string) *Query {
	q.query = append(q.query, "BEGIN "...)
	if mode != "" {
		q.query = append(q.query, strings.ToUpper(mode)...)
		q.query = append(q.query, " "...)
	}
	q.query = append(q.query, "TRANSACTION;"...)
	return q
}

// Commit is a function that returns a COMMIT TRANSACTION query
func (q *Query) Commit() *Query {
	q.query = append(q.query, " COMMIT TRANSACTION;"...)
	return q
}

// Rollback is a function that returns a ROLLBACK TRANSACTION query
func Rollback(savepoint string) *Query {
	return getQuery().Rollback(savepoint)
}

// Rollback is a function that returns a ROLLBACK TRANSACTION query
func (q *Query) Rollback(savepoint string) *Query {
	q.query = append(q.query, "ROLLBACK TRANSACTION"...)
	if savepoint != "" {
		q.query = append(q.query, " TO SAVEPOINT "...)
		q.query = append(q.query, savepoint...)
	}
	q.query = append(q.query, ";"...)
	return q
}

// Savepoint is a function that adds a SAVEPOINT statement to the query for the specified savepoint name
func Savepoint(name string) *Query {
	return getQuery().Savepoint(name)
}

// Savepoint is a function that adds a SAVEPOINT statement to the query for the specified savepoint name
func (q *Query) Savepoint(name string) *Query {
	q.query = append(q.query, "SAVEPOINT "...)
	q.query = append(q.query, name...)
	q.query = append(q.query, ";"...)
	return q
}

// ReleaseSavepoint is a function that adds a RELEASE SAVEPOINT statement to the query for the specified savepoint name
func ReleaseSavepoint(name string) *Query {
	return getQuery().ReleaseSavepoint(name)
}

// ReleaseSavepoint is a function that adds a RELEASE SAVEPOINT statement to the query for the specified savepoint name
func (q *Query) ReleaseSavepoint(name string) *Query {
	q.query = append(q.query, "RELEASE SAVEPOINT "...)
	q.query = append(q.query, name...)
	q.query = append(q.query, ";"...)
	return q
}

// AttachDatabase is a function that returns an ATTACH DATABASE query
func AttachDatabase(database, alias string) *Query {
	return getQuery().AttachDatabase(database, alias)
}

// AttachDatabase is a function that returns an ATTACH DATABASE query
func (q *Query) AttachDatabase(database, alias string) *Query {
	q.query = append(q.query, "ATTACH DATABASE "...)
	q.query = append(q.query, "'"...)
	q.query = append(q.query, database...)
	q.query = append(q.query, "'"...)
	q.query = append(q.query, " AS "...)
	q.query = append(q.query, alias...)
	q.query = append(q.query, ";"...)
	return q
}

// DetachDatabase is a function that returns a DETACH DATABASE query
func DetachDatabase(alias string) *Query {
	return getQuery().DetachDatabase(alias)
}

// DetachDatabase is a function that returns a DETACH DATABASE query
func (q *Query) DetachDatabase(alias string) *Query {
	q.query = append(q.query, "DETACH DATABASE "...)
	q.query = append(q.query, alias...)
	q.query = append(q.query, ";"...)
	return q
}

// Pragma is a function that returns a PRAGMA query
func Pragma(name, value string) *Query {
	return getQuery().Pragma(name, value)
}

// Pragma is a function that returns a PRAGMA query
func (q *Query) Pragma(name, value string) *Query {
	q.query = append(q.query, "PRAGMA "...)
	q.query = append(q.query, name...)
	if value != "" {
		q.query = append(q.query, " = "...)
		q.query = append(q.query, value...)
	}
	q.query = append(q.query, ";"...)
	return q
}

// Column is a struct representing a column in a CREATE TABLE statement
type Column struct {
	Name          string
	Type          string
	PrimaryKey    bool
	AutoIncrement bool
	Unique        bool
	NotNull       bool
	Check         string
	Default       string
	Collate       string
	References    string
	OnUpdate      string
	OnDelete      string
}

// CreateTable is a function that returns a CREATE TABLE query with the specified options
func CreateTable(name string, columns []Column, options ...string) *Query {
	return getQuery().CreateTable(name, columns, options...)
}

// CreateTable is a function that returns a CREATE TABLE query with the specified options
func (q *Query) CreateTable(name string, columns []Column, options ...string) *Query {
	q.query = append(q.query, "CREATE TABLE "...)
	q.query = append(q.query, name...)
	q.query = append(q.query, " ("...)
	for i, column := range columns {
		if i > 0 {
			q.query = append(q.query, ", "...)
		}
		q.query = append(q.query, column.Name...)
		q.query = append(q.query, " "...)
		q.query = append(q.query, column.Type...)
		if column.PrimaryKey {
			q.query = append(q.query, " PRIMARY KEY"...)
			if column.AutoIncrement {
				q.query = append(q.query, " AUTOINCREMENT"...)
			}
		}
		if column.Unique {
			q.query = append(q.query, " UNIQUE"...)
		}
		if column.NotNull {
			q.query = append(q.query, " NOT NULL"...)
		}
		if column.Check != "" {
			q.query = append(q.query, " CHECK ("...)
			q.query = append(q.query, column.Check...)
			q.query = append(q.query, ")"...)
		}
		if column.Default != "" {
			q.query = append(q.query, " DEFAULT "...)
			q.query = append(q.query, column.Default...)
		}
		if column.Collate != "" {
			q.query = append(q.query, " COLLATE "...)
			q.query = append(q.query, column.Collate...)
		}
		if column.References != "" {
			q.query = append(q.query, " REFERENCES "...)
			q.query = append(q.query, column.References...)
			if column.OnUpdate != "" {
				q.query = append(q.query, " ON UPDATE "...)
				q.query = append(q.query, column.OnUpdate...)
			}
			if column.OnDelete != "" {
				q.query = append(q.query, " ON DELETE "...)
				q.query = append(q.query, column.OnDelete...)
			}
		}
	}
	q.query = append(q.query, ")"...)
	if len(options) > 0 {
		q.query = append(q.query, " "...)
		q.query = append(q.query, strings.Join(options, " ")...)
	}
	q.query = append(q.query, ";"...)
	return q
}

// DropTable is a function that returns a DROP TABLE query for the specified table
func DropTable(table string) *Query {
	return getQuery().DropTable(table)
}

// DropTable is a function that returns a DROP TABLE query for the specified table
func (q *Query) DropTable(table string) *Query {
	q.query = append(q.query, "DROP TABLE "...)
	q.query = append(q.query, table...)
	q.query = append(q.query, ";"...)
	return q
}

// AlterTableName is a function that returns a RENAME TABLE query
func AlterTable(table string) *Query {
	return getQuery().AlterTable(table)
}

// AlterTableName builds the query string for a RENAME TABLE statement
func (q *Query) AlterTable(table string) *Query {
	q.query = append(q.query, "ALTER TABLE "...)
	q.query = append(q.query, table...)
	return q
}

// RenameTo is a function that returns a RENAME TO query
func (q *Query) RenameTo(newName string) *Query {
	q.query = append(q.query, " RENAME TO "...)
	q.query = append(q.query, newName...)
	q.query = append(q.query, ";"...)
	return q
}

// RenameColumn is a function that returns a RENAME COLUMN query
func (q *Query) RenameColumn(oldName, newName string) *Query {
	q.query = append(q.query, " RENAME COLUMN "...)
	q.query = append(q.query, oldName...)
	q.query = append(q.query, " TO "...)
	q.query = append(q.query, newName...)
	q.query = append(q.query, ";"...)
	return q
}

// AddColumn is a function that returns an ADD COLUMN query
func (q *Query) AddColumn(column Column, options ...string) *Query {
	q.query = append(q.query, " ADD COLUMN "...)
	q.query = append(q.query, column.Name...)
	q.query = append(q.query, " "...)
	q.query = append(q.query, column.Type...)
	if column.PrimaryKey {
		q.query = append(q.query, " PRIMARY KEY"...)
		if column.AutoIncrement {
			q.query = append(q.query, " AUTOINCREMENT"...)
		}
	}
	if column.Unique {
		q.query = append(q.query, " UNIQUE"...)
	}
	if column.NotNull {
		q.query = append(q.query, " NOT NULL"...)
	}
	if column.Check != "" {
		q.query = append(q.query, " CHECK ("...)
		q.query = append(q.query, column.Check...)
		q.query = append(q.query, ")"...)
	}
	if column.Default != "" {
		q.query = append(q.query, " DEFAULT "...)
		q.query = append(q.query, column.Default...)
	}
	if column.Collate != "" {
		q.query = append(q.query, " COLLATE "...)
		q.query = append(q.query, column.Collate...)
	}
	if column.References != "" {
		q.query = append(q.query, " REFERENCES "...)
		q.query = append(q.query, column.References...)
		if column.OnUpdate != "" {
			q.query = append(q.query, " ON UPDATE "...)
			q.query = append(q.query, column.OnUpdate...)
		}
		if column.OnDelete != "" {
			q.query = append(q.query, " ON DELETE "...)
			q.query = append(q.query, column.OnDelete...)
		}
	}
	if len(options) > 0 {
		q.query = append(q.query, " "...)
		q.query = append(q.query, strings.Join(options, " ")...)
	}
	q.query = append(q.query, ";"...)
	return q
}

// DropColumn is a function that returns a DROP COLUMN query
func (q *Query) DropColumn(columnName string) *Query {
	q.query = append(q.query, " DROP COLUMN "...)
	q.query = append(q.query, columnName...)
	q.query = append(q.query, ";"...)
	return q
}

// CreateIndex is a function that returns a CREATE INDEX query for the specified index and columns
func CreateIndex(name, table string, columns []string, unique bool) *Query {
	return getQuery().CreateIndex(name, table, columns, unique)
}

// CreateIndex is a function that returns a CREATE INDEX query for the specified index and columns
func (q *Query) CreateIndex(name, table string, columns []string, unique bool) *Query {
	q.query = append(q.query, "CREATE "...)
	if unique {
		q.query = append(q.query, "UNIQUE "...)
	}
	q.query = append(q.query, "INDEX "...)
	q.query = append(q.query, name...)
	q.query = append(q.query, " ON "...)
	q.query = append(q.query, table...)
	q.query = append(q.query, " ("...)
	q.query = append(q.query, strings.Join(columns, ", ")...)
	q.query = append(q.query, ");"...)
	return q
}

// DropIndex is a function that returns a DROP INDEX query for the specified index
func DropIndex(name string) *Query {
	return getQuery().DropIndex(name)
}

// DropIndex is a function that returns a DROP INDEX query for the specified index
func (q *Query) DropIndex(name string) *Query {
	q.query = append(q.query, "DROP INDEX "...)
	q.query = append(q.query, name...)
	q.query = append(q.query, ";"...)
	return q
}

// CreateView is a function that returns a CREATE VIEW query for the specified view and SQL statement
func CreateView(name, query string) *Query {
	return getQuery().CreateView(name, query)
}

// CreateView is a function that returns a CREATE VIEW query for the specified view and SQL statement
func (q *Query) CreateView(name, query string) *Query {
	q.query = append(q.query, "CREATE VIEW "...)
	q.query = append(q.query, name...)
	q.query = append(q.query, " AS "...)
	q.query = append(q.query, query...)
	q.query = append(q.query, ";"...)
	return q
}

// DropView is a function that returns a DROP VIEW query for the specified view
func DropView(name string) *Query {
	return getQuery().DropView(name)
}

// DropView is a function that returns a DROP VIEW query for the specified view
func (q *Query) DropView(name string) *Query {
	q.query = append(q.query, "DROP VIEW "...)
	q.query = append(q.query, name...)
	q.query = append(q.query, ";"...)
	return q
}

// CreateTrigger is a function that returns a CREATE TRIGGER query for the specified trigger
func CreateTrigger(name, table, when, event, actions string) *Query {
	return getQuery().CreateTrigger(name, table, when, event, actions)
}

// CreateTrigger is a function that returns a CREATE TRIGGER query for the specified trigger
func (q *Query) CreateTrigger(name, table, when, event, actions string) *Query {
	q.query = append(q.query, "CREATE TRIGGER "...)
	q.query = append(q.query, name...)
	q.query = append(q.query, " "...)
	q.query = append(q.query, when...)
	q.query = append(q.query, " "...)
	q.query = append(q.query, event...)
	q.query = append(q.query, " ON "...)
	q.query = append(q.query, table...)
	q.query = append(q.query, " "...)
	q.query = append(q.query, actions...)
	q.query = append(q.query, ";"...)
	return q
}

// DropTrigger is a function that returns a DROP TRIGGER query for the specified trigger
func DropTrigger(name string) *Query {
	return getQuery().DropTrigger(name)
}

// DropTrigger is a function that returns a DROP TRIGGER query for the specified trigger
func (q *Query) DropTrigger(name string) *Query {
	q.query = append(q.query, "DROP TRIGGER "...)
	q.query = append(q.query, name...)
	q.query = append(q.query, ";"...)
	return q
}

// DeleteFrom is a function to start building a DELETE FROM query statement
func DeleteFrom(table string) *Query {
	return getQuery().DeleteFrom(table)
}

// DeleteFrom is a method for the Query struct and builds the query string for a DELETE statement
func (q *Query) DeleteFrom(table string) *Query {
	q.query = append(q.query, "DELETE FROM "...)
	q.query = append(q.query, table...)
	return q
}

// InsertInto is a function to start building an INSERT INTO query statement
func InsertInto(table string) *Query {
	return getQuery().InsertInto(table)
}

// InsertInto builds the query string for an INSERT INTO statement
func (q *Query) InsertInto(table string) *Query {
	q.query = append(q.query, "INSERT INTO "...)
	q.query = append(q.query, table...)
	return q
}

// Columns builds the query string for the COLUMNS clause in an INSERT INTO statement
func (q *Query) Columns(columns ...string) *Query {
	q.query = append(q.query, " ("...)
	for i, column := range columns {
		if i > 0 {
			q.query = append(q.query, ", "...)
		}
		q.query = append(q.query, column...)
	}
	q.query = append(q.query, ')')
	return q
}

// Values builds the query string for the VALUES clause in an INSERT INTO statement
func (q *Query) Values(values ...any) *Query {
	q.query = append(q.query, " VALUES"...)
	q.query = append(q.query, " ("...)
	for i := range values {
		if i > 0 {
			q.query = append(q.query, ", "...)
		}
		q.query = append(q.query, '?')
	}
	q.query = append(q.query, ')')
	q.args = append(q.args, values...)
	return q
}

// OnConflict builds the query string for the ON CONFLICT clause in an INSERT INTO statement
func (q *Query) OnConflict(columns ...string) *Query {
	q.query = append(q.query, " ON CONFLICT ("...)
	for i, column := range columns {
		if i > 0 {
			q.query = append(q.query, ", "...)
		}
		q.query = append(q.query, column...)
	}
	q.query = append(q.query, ')')
	return q
}

// Do is a function to start building a DO query statement
func (q *Query) Do() *Query {
	q.query = append(q.query, " DO "...)
	return q
}

// Nothing is a function that returns a NOTHING clause for the specified fields
func (q *Query) Nothing() *Query {
	q.query = append(q.query, "NOTHING"...)
	return q
}

// Update is a function to start building an UPDATE query statement
func Update(table, condition string) *Query {
	return getQuery().Update(table, condition)
}

// Update is a function that returns an UPDATE query for the specified table and condition
func (q *Query) Update(table, condition string) *Query {
	q.query = append(q.query, "UPDATE "...)
	q.query = append(q.query, table...)
	if condition != "" {
		q.query = append(q.query, " OR "...)
		q.query = append(q.query, condition...)
	}
	return q
}

// Field is a struct that represents a field in a table
type Field struct {
	Name  string
	Value any
}

// Set is a function that returns a SET clause for the specified fields
func (q *Query) Set(fields []*Field) *Query {
	q.query = append(q.query, " SET "...)
	for i, field := range fields {
		q.query = append(q.query, field.Name...)
		q.query = append(q.query, " = ?"...)
		if i < len(fields)-1 {
			q.query = append(q.query, ", "...)
		}
		q.args = append(q.args, field.Value)
	}
	return q
}

// Select is a function to start building a SELECT query statement
func Select(conditions ...string) *Query {
	return getQuery().Select(conditions...)
}

// Select is a function that returns a SELECT query for the specified columns and tables, with optional conditions
func (q *Query) Select(conditions ...string) *Query {
	q.query = append(q.query, "SELECT "...)
	for i, condition := range conditions {
		if i > 0 {
			q.query = append(q.query, ", "...)
		}
		q.query = append(q.query, condition...)
	}
	return q
}

// From is a function that returns a FROM clause for the specified tables
func (q *Query) From(tables ...string) *Query {
	q.query = append(q.query, " FROM "...)
	for i, table := range tables {
		if i > 0 {
			q.query = append(q.query, ", "...)
		}
		q.query = append(q.query, table...)
	}
	return q
}

// Where is a function that returns a WHERE clause for the specified expression and value
func (q *Query) Where(expr string) *Query {
	q.query = append(q.query, " WHERE "...)
	q.query = append(q.query, expr...)
	return q
}

// Join is a function that returns a JOIN clause for the specified tables
func (q *Query) Join(table, condition string) *Query {
	q.query = append(q.query, " JOIN "...)
	q.query = append(q.query, table...)
	q.query = append(q.query, " ON "...)
	q.query = append(q.query, condition...)
	return q
}

// LeftJoin is a function that returns a LEFT JOIN clause for the specified tables
func (q *Query) LeftJoin(table, condition string) *Query {
	q.query = append(q.query, " LEFT JOIN "...)
	q.query = append(q.query, table...)
	q.query = append(q.query, " ON "...)
	q.query = append(q.query, condition...)
	return q
}

// RightJoin is a function that returns a RIGHT JOIN clause for the specified tables
func (q *Query) RightJoin(table, condition string) *Query {
	q.query = append(q.query, " RIGHT JOIN "...)
	q.query = append(q.query, table...)
	q.query = append(q.query, " ON "...)
	q.query = append(q.query, condition...)
	return q
}

// FullJoin is a function that returns a FULL JOIN clause for the specified tables
func (q *Query) FullJoin(table, condition string) *Query {
	q.query = append(q.query, " FULL JOIN "...)
	q.query = append(q.query, table...)
	q.query = append(q.query, " ON "...)
	q.query = append(q.query, condition...)
	return q
}

// Having is a function that adds a HAVING clause to the query for the specified conditions
func (q *Query) Having(condition string) *Query {
	q.query = append(q.query, " HAVING "...)
	q.query = append(q.query, condition...)
	return q
}

// GroupBy is a function that returns a GROUP BY clause for the specified columns
func (q *Query) GroupBy(conditions ...string) *Query {
	q.query = append(q.query, " GROUP BY "...)
	for i, condition := range conditions {
		if i > 0 {
			q.query = append(q.query, ", "...)
		}
		q.query = append(q.query, condition...)
	}
	return q
}

// OrderBy is a function that returns an ORDER BY clause for the specified columns and sort order
func (q *Query) OrderBy(columns ...string) *Query {
	q.query = append(q.query, " ORDER BY "...)
	for i, column := range columns {
		if i > 0 {
			q.query = append(q.query, ", "...)
		}
		q.query = append(q.query, column...)
	}
	return q
}

// IndexBy is a function that returns an INDEX BY clause for the specified index
func (q *Query) IndexBy(name string) *Query {
	q.query = append(q.query, " INDEX BY "...)
	q.query = append(q.query, name...)
	return q
}

// NotIndex is a function that returns a NOT INDEX clause
func (q *Query) NotIndex() *Query {
	q.query = append(q.query, " NOT INDEX"...)
	return q
}

// Reindex is a function that returns a REINDEX clause
func (q *Query) Reindex(name string) *Query {
	q.query = append(q.query, " REINDEX "...)
	q.query = append(q.query, name...)
	return q
}

// Limit is a function that specifies the maximum number of rows to return
func (q *Query) Limit(limit int) *Query {
	q.query = append(q.query, " LIMIT ?"...)
	q.args = append(q.args, limit)
	return q
}

// Offset is a function that specifies the number of rows to skip before starting to return rows
func (q *Query) Offset(offset int) *Query {
	q.query = append(q.query, " OFFSET ?"...)
	q.args = append(q.args, offset)
	return q
}

// Paginate is a function that returns a LIMIT and OFFSET clause for the specified page and page size
func (q *Query) Paginate(page, pageSize int) *Query {
	if page < 1 {
		page = 1
	}
	if pageSize < 1 {
		pageSize = 1
	}
	if page > 1 {
		q.Offset((page - 1) * pageSize)
	}
	q.Limit(pageSize)
	return q
}

// Returning is a function that returns a RETURNING clause for the specified columns
func (q *Query) Returning(columns ...string) *Query {
	q.query = append(q.query, " RETURNING "...)
	for i, c := range columns {
		if i > 0 {
			q.query = append(q.query, ", "...)
		}
		q.query = append(q.query, c...)
	}
	return q
}

// With is a function that returns a WITH clause for the specified name and query
func With(name string, query *Query) *Query {
	return getQuery().With(name, query)
}

// With is a function that returns a WITH clause for the specified name and query
func (q *Query) With(name string, query *Query) *Query {
	q.query = append(q.query, "WITH "...)
	q.query = append(q.query, name...)
	q.query = append(q.query, " AS ("...)
	q.query = append(q.query, query.query...)
	q.query = append(q.query, ") "...)
	q.args = append(q.args, query.args...)
	return q
}

// And is a function that returns an AND WHERE clause for the specified expression and value
func (q *Query) And(query *Query) *Query {
	q.query = append(q.query, " AND "...)
	q.query = append(q.query, query.query...)
	q.args = append(q.args, query.args...)
	return q
}

// Or is a function that returns an OR WHERE clause for the specified expression and value
func (q *Query) Or(query *Query) *Query {
	q.query = append(q.query, " OR "...)
	q.query = append(q.query, query.query...)
	q.args = append(q.args, query.args...)
	return q
}

// Not is a function that returns a NOT WHERE clause for the specified expression and value
func (q *Query) Not(query *Query) *Query {
	q.query = append(q.query, " NOT "...)
	q.query = append(q.query, query.query...)
	q.args = append(q.args, query.args...)
	return q
}

// Like is a function that returns a LIKE WHERE clause for the specified column and value
func (q *Query) Like(column string) *Query {
	q.query = append(q.query, column...)
	q.query = append(q.query, " LIKE ?"...)
	return q
}

// In is a function that returns an IN WHERE clause for the specified column and values
func (q *Query) In(column string, values ...any) *Query {
	q.query = append(q.query, column...)
	q.query = append(q.query, " IN ("...)
	for i, value := range values {
		if i > 0 {
			q.query = append(q.query, ", "...)
		}
		q.query = append(q.query, '?')
		q.args = append(q.args, value)
	}
	q.query = append(q.query, ')')
	return q
}

// Asc is a function that specifies ascending sort order for the most recently specified column in the ORDER BY clause
func (q *Query) Asc() *Query {
	q.query = append(q.query, " ASC"...)
	return q
}

// Desc is a function that specifies descending sort order for the most recently specified column in the ORDER BY clause
func (q *Query) Desc() *Query {
	q.query = append(q.query, " DESC"...)
	return q
}

// Vacuum is a function that returns a VACUUM statement for the specified schema and file
func Vacuum(schema, file string) *Query {
	return getQuery().Vacuum(schema, file)
}

// Vacuum is a function that returns a VACUUM statement for the specified schema and file
func (q *Query) Vacuum(schema, file string) *Query {
	q.query = append(q.query, "VACUUM"...)
	if schema != "" {
		q.query = append(q.query, " "...)
		q.query = append(q.query, schema...)
	}
	if file != "" {
		q.query = append(q.query, " INTO "...)
		q.query = append(q.query, file...)
	}
	q.query = append(q.query, ";"...)
	return q
}

// Raw is a function that returns a raw query string and arguments
func (q *Query) Raw(query string, args ...any) *Query {
	q.query = append(q.query, query...)
	q.args = append(q.args, args...)
	return q
}

// String is a function that returns the query string
func (q *Query) String() string {
	query := string(q.query)
	q.Reset()
	return query
}

// Args is a function that appends arguments to the query
func (q *Query) Args(args ...any) *Query {
	q.args = append(q.args, args...)
	return q
}

// Query is a function that returns the query string and arguments
func (q *Query) Query() (string, []any) {
	query := string(q.query)
	args := q.args

	q.Reset()
	return query, args
}

// Reset is a function that resets the query string and arguments
func (q *Query) Reset() {
	q.query = q.query[:0]
	q.args = q.args[:0]
	queryPool.Put(q)
}

var queryPool = sync.Pool{New: func() any {
	return &Query{
		query: make([]byte, 0, 8),
		args:  make([]any, 0, 8),
	}
}}

func getQuery() *Query {
	return queryPool.Get().(*Query)
}
