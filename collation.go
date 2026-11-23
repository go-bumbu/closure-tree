package closuretree

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
)

// The collations Migrate gives the tenant columns on gorm's "mysql" driver: binary, so tenants
// that differ in case or accents are different, and NO PAD, so a trailing space counts too. That
// is how PostgreSQL and SQLite compare them by default. On MariaDB 11.8 utf8mb4_0900_bin is an alias
// of utf8mb4_nopad_bin, but information_schema reports a column by the name it was given, so the
// library uses and checks the one name utf8mb4_nopad_bin.
const (
	tenantCollationMariaDB = "utf8mb4_nopad_bin"
	tenantCollationMySQL   = "utf8mb4_0900_bin" // MySQL (accepted, untested) has it from 8.0.17
)

// tenantCollationOf returns the collation of the tenant columns for the server of version, as
// SELECT VERSION() returns it.
func tenantCollationOf(version string) string {
	if isMariaDBVersion(version) {
		return tenantCollationMariaDB
	}
	return tenantCollationMySQL
}

// ErrTenantCollation is returned by Migrate on MariaDB when the tenant columns of the tree's four
// tables do not share one collation; the error names each column and its collation. The tables would
// compare tenants differently, so that DeleteTenant could delete a tenant's nodes and keep their
// closure rows. See Tree for the statement that gives all four the exact collation.
var ErrTenantCollation = errors.New("the tenant columns of the tree do not share one collation")

// tenantColumn is the tenant column of one of the tree's tables, as information_schema describes it.
type tenantColumn struct {
	Table     string `gorm:"column:table_name"`
	Type      string `gorm:"column:column_type"`
	Collation string `gorm:"column:collation_name"`
}

// tenantColumnType is the only column type alterTenantCollationSQL writes into its statement.
var tenantColumnType = regexp.MustCompile(`^varchar\(\d+\)$`)

// alterTenantCollationSQL returns the statement that gives the tenant column of table, already
// quoted, the collation collation and keeps its type columnType, which must be a varchar.
func alterTenantCollationSQL(table, columnType, collation string) (string, error) {
	if !tenantColumnType.MatchString(columnType) {
		return "", fmt.Errorf("unexpected type %q of the tenant column of %s: want varchar(n)", columnType, table)
	}
	return fmt.Sprintf("ALTER TABLE %s MODIFY COLUMN tenant %s CHARACTER SET utf8mb4 COLLATE %s NOT NULL",
		table, columnType, collation), nil
}

// inexactTenantColumns returns the columns of cols whose collation is not collation.
func inexactTenantColumns(cols []tenantColumn, collation string) []tenantColumn {
	var out []tenantColumn
	for _, c := range cols {
		if c.Collation != collation {
			out = append(out, c)
		}
	}
	return out
}

// sameTenantCollation returns ErrTenantCollation, naming each column and its collation, unless all
// columns of cols have the same collation.
func sameTenantCollation(cols []tenantColumn) error {
	same := true
	for _, c := range cols {
		same = same && c.Collation == cols[0].Collation
	}
	if same {
		return nil
	}
	parts := make([]string, len(cols))
	for i, c := range cols {
		parts[i] = c.Table + ".tenant " + c.Collation
	}
	return fmt.Errorf("%w: %s", ErrTenantCollation, strings.Join(parts, ", "))
}

// ensureTenantCollation is the last step of Migrate. On MariaDB it gives the tenant column of each of
// the tree's four tables the collation utf8mb4_nopad_bin (utf8mb4_0900_bin on MySQL, see
// tenantCollationOf) while none of the tables has a row; a tree with rows keeps the collation it
// has, and gets ErrTenantCollation when its tenant columns do not share one. The other dialects
// compare tenants exactly already.
func (ct *Tree) ensureTenantCollation() error {
	if ct.dialect != dialectMariaDB {
		return nil
	}
	version, err := serverVersion(ct.db)
	if err != nil {
		return err
	}
	collation := tenantCollationOf(version)
	cols, err := ct.tenantColumns()
	if err != nil {
		return err
	}
	inexact := inexactTenantColumns(cols, collation)
	if len(inexact) == 0 {
		return nil
	}
	empty, err := ct.treeIsEmpty()
	if err != nil {
		return err
	}
	if !empty {
		return sameTenantCollation(cols)
	}
	for _, c := range inexact {
		stmt, err := alterTenantCollationSQL(ct.quoteTable(c.Table), c.Type, collation)
		if err != nil {
			return err
		}
		if err := ct.db.Exec(stmt).Error; err != nil {
			return fmt.Errorf("unable to set the collation of the tenant column of %s: %w", c.Table, err)
		}
	}
	return nil
}

// treeTables returns the names of the tree's four tables.
func (ct *Tree) treeTables() []string {
	return []string{ct.nodesTbl, ct.relationsTbl, ct.metaTbl, ct.lockTbl}
}

// tenantColumns reads the type and collation of the tenant column of each of the tree's four
// tables from information_schema (MariaDB), ordered by table name.
func (ct *Tree) tenantColumns() ([]tenantColumn, error) {
	tables := ct.treeTables()
	var cols []tenantColumn
	if err := ct.db.Raw(`SELECT TABLE_NAME AS table_name, COLUMN_TYPE AS column_type,
		COALESCE(COLLATION_NAME, '') AS collation_name
		FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA = DATABASE() AND COLUMN_NAME = 'tenant' AND TABLE_NAME IN ?
		ORDER BY TABLE_NAME`, tables).Scan(&cols).Error; err != nil {
		return nil, fmt.Errorf("unable to read the tenant columns: %w", err)
	}
	if len(cols) != len(tables) {
		return nil, fmt.Errorf("unable to read the tenant columns: found %d of the %d tables %v", len(cols), len(tables), tables)
	}
	return cols, nil
}

// treeIsEmpty reports whether none of the tree's four tables has a row.
func (ct *Tree) treeIsEmpty() (bool, error) {
	for _, tbl := range ct.treeTables() {
		var one int
		res := ct.db.Raw("SELECT 1 FROM " + ct.quoteTable(tbl) + " LIMIT 1").Scan(&one)
		if res.Error != nil {
			return false, fmt.Errorf("unable to check for rows in %s: %w", tbl, res.Error)
		}
		if res.RowsAffected > 0 {
			return false, nil
		}
	}
	return true, nil
}
