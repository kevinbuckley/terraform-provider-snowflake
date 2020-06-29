package snowflake

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/jmoiron/sqlx"
)

// TableBuilder abstracts the creation of SQL queries for a Snowflake Table
type TableBuilder struct {
	name    string
	db      string
	schema  string
	comment string
}

// QualifiedName prepends the db and schema if set and escapes everything nicely
func (vb *TableBuilder) QualifiedName() string {
	var n strings.Builder

	if vb.db != "" && vb.schema != "" {
		n.WriteString(fmt.Sprintf(`"%v"."%v".`, vb.db, vb.schema))
	}

	if vb.db != "" && vb.schema == "" {
		n.WriteString(fmt.Sprintf(`"%v"..`, vb.db))
	}

	if vb.db == "" && vb.schema != "" {
		n.WriteString(fmt.Sprintf(`"%v".`, vb.schema))
	}

	n.WriteString(fmt.Sprintf(`"%v"`, vb.name))

	return n.String()
}

// WithComment adds a comment to the TableBuilder
func (vb *TableBuilder) WithComment(c string) *TableBuilder {
	vb.comment = c
	return vb
}

// WithDB adds the name of the database to the TableBuilder
func (vb *TableBuilder) WithDB(db string) *TableBuilder {
	vb.db = db
	return vb
}

// WithSchema adds the name of the schema to the TableBuilder
func (vb *TableBuilder) WithSchema(s string) *TableBuilder {
	vb.schema = s
	return vb
}

// Table returns a pointer to a Builder that abstracts the DDL operations for a table.
//
// Supported DDL operations are:
//   - CREATE TABLE
//   - ALTER TABLE
//   - DROP TABLE
//   - SHOW TABLES
//   - DESCRIBE TABLE
//
func Table(name string) *TableBuilder {
	return &TableBuilder{
		name: name,
	}
}

// Create returns the SQL query that will create a new table.
func (vb *TableBuilder) Create() string {
	var q strings.Builder

	q.WriteString("CREATE OR REPLACE")

	q.WriteString(fmt.Sprintf(` TABLE %v(placeholder varchar(100))`, vb.QualifiedName()))

	if vb.comment != "" {
		q.WriteString(fmt.Sprintf(" COMMENT = '%v'", vb.comment))
	}

	return q.String()
}

// Rename returns the SQL query that will rename the table.
func (vb *TableBuilder) Rename(newName string) string {
	oldName := vb.QualifiedName()
	vb.name = newName
	return fmt.Sprintf(`ALTER TABLE %v RENAME TO %v`, oldName, vb.QualifiedName())
}

// ChangeComment returns the SQL query that will update the comment on the table.
// Note that comment is the only parameter, if more are released this should be
// abstracted as per the generic builder.
func (vb *TableBuilder) ChangeComment(c string) string {
	return fmt.Sprintf(`ALTER TABLE %v SET COMMENT = '%v'`, vb.QualifiedName(), c)
}

// RemoveComment returns the SQL query that will remove the comment on the table.
// Note that comment is the only parameter, if more are released this should be
// abstracted as per the generic builder.
func (vb *TableBuilder) RemoveComment() string {
	return fmt.Sprintf(`ALTER TABLE %v UNSET COMMENT`, vb.QualifiedName())
}

// Show returns the SQL query that will show the row representing this table.
func (vb *TableBuilder) Show() string {
	if vb.db == "" {
		return fmt.Sprintf(`SHOW TABLES LIKE '%v'`, vb.name)
	}
	return fmt.Sprintf(`SHOW TABLES LIKE '%v' IN DATABASE "%v"`, vb.name, vb.db)
}

// Drop returns the SQL query that will drop the row representing this table.
func (vb *TableBuilder) Drop() string {
	return fmt.Sprintf(`DROP TABLE %v`, vb.QualifiedName())
}

type table struct {
	Comment      sql.NullString `db:"comment"`
	Name         sql.NullString `db:"name"`
	SchemaName   sql.NullString `db:"schema_name"`
	DatabaseName sql.NullString `db:"database_name"`
}

// ScanTable scans a table yo
func ScanTable(row *sqlx.Row) (*table, error) {
	r := &table{}
	err := row.StructScan(r)
	return r, err
}
