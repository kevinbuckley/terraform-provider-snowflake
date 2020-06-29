package resources

import (
	"database/sql"
	"fmt"
	"log"
	"regexp"
	"strings"

	"github.com/chanzuckerberg/terraform-provider-snowflake/pkg/snowflake"
	"github.com/hashicorp/terraform-plugin-sdk/helper/schema"
	"github.com/pkg/errors"
)

var tableSpace = regexp.MustCompile(`\s+`)

var tableSchema = map[string]*schema.Schema{
	"name": &schema.Schema{
		Type:        schema.TypeString,
		Required:    true,
		Description: "Specifies the identifier for the table; must be unique for the schema in which the table is created. Don't use the | character.",
	},
	"database": &schema.Schema{
		Type:        schema.TypeString,
		Required:    true,
		Description: "The database in which to create the table. Don't use the | character.",
		ForceNew:    true,
	},
	"schema": &schema.Schema{
		Type:        schema.TypeString,
		Optional:    true,
		Default:     "PUBLIC",
		Description: "The schema in which to create the table. Don't use the | character.",
		ForceNew:    true,
	},
	"comment": &schema.Schema{
		Type:        schema.TypeString,
		Optional:    true,
		Description: "Specifies a comment for the table.",
	},
}

func tableNormalizeQuery(str string) string {
	return strings.TrimSpace(tableSpace.ReplaceAllString(str, " "))
}

// tableDiffSuppressStatement will suppress diffs between statemens if they differ in only case or in
// runs of whitespace (\s+ = \s). This is needed because the snowflake api does not faithfully
// round-trip queries so we cannot do a simple character-wise comparison to detect changes.
//
// Warnings: We will have false positives in cases where a change in case or run of whitespace is
// semantically significant.
//
// If we can find a sql parser that can handle the snowflake dialect then we should switch to parsing
// queries and either comparing ASTs or emiting a canonical serialization for comparison. I couldnt'
// find such a library.
func tableDiffSuppressStatement(_, old, new string, d *schema.ResourceData) bool {
	return strings.EqualFold(tableNormalizeQuery(old), tableNormalizeQuery(new))
}

// Table returns a pointer to the resource representing a table
func Table() *schema.Resource {
	return &schema.Resource{
		Create: CreateTable,
		Read:   ReadTable,
		Update: UpdateTable,
		Delete: DeleteTable,
		Exists: TableExists,

		Schema: tableSchema,
		Importer: &schema.ResourceImporter{
			State: schema.ImportStatePassthrough,
		},
	}
}

// CreateTable implements schema.CreateFunc
func CreateTable(data *schema.ResourceData, meta interface{}) error {
	db := meta.(*sql.DB)
	name := data.Get("name").(string)
	schema := data.Get("schema").(string)
	database := data.Get("database").(string)

	builder := snowflake.Table(name).WithDB(database).WithSchema(schema)

	if v, ok := data.GetOk("comment"); ok {
		builder.WithComment(v.(string))
	}

	if v, ok := data.GetOk("schema"); ok {
		builder.WithSchema(v.(string))
	}

	q := builder.Create()
	log.Print("[DEBUG] xxx ", q)
	err := snowflake.Exec(db, q)
	if err != nil {
		return errors.Wrapf(err, "error creating table %v", name)
	}

	data.SetId(fmt.Sprintf("%v|%v|%v", database, schema, name))

	return ReadTable(data, meta)
}

// ReadTable implements schema.ReadFunc
func ReadTable(data *schema.ResourceData, meta interface{}) error {
	db := meta.(*sql.DB)
	dbName, schema, table, err := splitTableID(data.Id())
	if err != nil {
		return err
	}

	q := snowflake.Table(table).WithDB(dbName).WithSchema(schema).Show()
	row := snowflake.QueryRow(db, q)
	v, err := snowflake.ScanTable(row)
	if err != nil {
		return err
	}

	err = data.Set("name", v.Name.String)
	if err != nil {
		return err
	}

	err = data.Set("schema", v.SchemaName.String)
	if err != nil {
		return err
	}

	return data.Set("database", v.DatabaseName.String)
}

// UpdateTable implements schema.UpdateFunc
func UpdateTable(data *schema.ResourceData, meta interface{}) error {
	// https://www.terraform.io/docs/extend/writing-custom-providers.html#error-handling-amp-partial-state
	data.Partial(true)

	dbName, schema, table, err := splitTableID(data.Id())
	if err != nil {
		return err
	}

	builder := snowflake.Table(table).WithDB(dbName).WithSchema(schema)

	db := meta.(*sql.DB)
	if data.HasChange("name") {
		_, name := data.GetChange("name")

		q := builder.Rename(name.(string))
		err := snowflake.Exec(db, q)
		if err != nil {
			return errors.Wrapf(err, "error renaming table %v", data.Id())
		}

		data.SetId(fmt.Sprintf("%v|%v|%v", dbName, schema, name.(string)))
		data.SetPartial("name")
	}

	if data.HasChange("comment") {
		_, comment := data.GetChange("comment")

		if c := comment.(string); c == "" {
			q := builder.RemoveComment()
			err := snowflake.Exec(db, q)
			if err != nil {
				return errors.Wrapf(err, "error unsetting comment for table %v", data.Id())
			}
		} else {
			q := builder.ChangeComment(c)
			err := snowflake.Exec(db, q)
			if err != nil {
				return errors.Wrapf(err, "error updating comment for table %v", data.Id())
			}
		}

		data.SetPartial("comment")
	}

	return ReadTable(data, meta)
}

// DeleteTable implements schema.DeleteFunc
func DeleteTable(data *schema.ResourceData, meta interface{}) error {
	db := meta.(*sql.DB)
	dbName, schema, table, err := splitTableID(data.Id())
	if err != nil {
		return err
	}

	q := snowflake.Table(table).WithDB(dbName).WithSchema(schema).Drop()

	err = snowflake.Exec(db, q)
	if err != nil {
		return errors.Wrapf(err, "error deleting table %v", data.Id())
	}

	data.SetId("")

	return nil
}

// TableExists implements schema.ExistsFunc
func TableExists(data *schema.ResourceData, meta interface{}) (bool, error) {
	db := meta.(*sql.DB)
	dbName, schema, table, err := splitTableID(data.Id())
	if err != nil {
		return false, err
	}

	q := snowflake.Table(table).WithDB(dbName).WithSchema(schema).Show()
	rows, err := db.Query(q)
	if err != nil {
		return false, err
	}
	defer rows.Close()

	if rows.Next() {
		return true, nil
	}

	return false, nil
}

// splitTableID takes the <database_name>|<schema_name>|<table_name> ID and returns the database
// name, schema name and table name.
func splitTableID(v string) (string, string, string, error) {
	arr := strings.Split(v, "|")
	if len(arr) != 3 {
		return "", "", "", fmt.Errorf("ID %v is invalid", v)
	}

	return arr[0], arr[1], arr[2], nil
}
