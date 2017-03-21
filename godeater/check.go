// godeater kills omnipotence before it can begin
package godeater

import (
	"regexp"
	"strings"

	"github.com/flike/kingshard/core/golog"
	"github.com/flike/kingshard/sqlparser"
)

var (
	SafeTables  = map[string]bool{"schools": true, "announcements": true}
	SafeColumns []*regexp.Regexp
)

func init() {
	SafeColumns = append(
		SafeColumns,
		regexp.MustCompile(".*\\.id"),
		regexp.MustCompile(".*\\.school_id"),
		regexp.MustCompile(".*\\.group_id"),
		regexp.MustCompile(".*\\.term_id"),
		regexp.MustCompile(".*\\.user_id"),
		regexp.MustCompile(".*\\.persona_id"),
		regexp.MustCompile(".*\\.creator_id"),
		regexp.MustCompile(".*\\.created_by"),
		regexp.MustCompile(".*\\.updated_by"),
		regexp.MustCompile(".*\\.owner_id"),
		regexp.MustCompile(".*\\.advisor_id"),
		regexp.MustCompile(".*\\.tutor_id"),
		regexp.MustCompile("calendar_instances\\.organizer_id"),
		regexp.MustCompile("availabilities\\.available_id"),
		regexp.MustCompile("messages\\.recipient_id"),
		regexp.MustCompile("messages\\.sender_id"),
		regexp.MustCompile("allowances\\.allowable_id"),
		regexp.MustCompile("sessions\\.session_id"),
		regexp.MustCompile("direct_relationships\\.child_id"),
		regexp.MustCompile("direct_relationships\\.parent_id"),
	)
}

func Check(stmt sqlparser.Statement, id uint32) bool {
	tables := GetTables(stmt)
	allSafe := true
	for _, table := range tables {
		if !SafeTables[table] {
			allSafe = false
		}
	}
	if allSafe {
		return true
	}

	columns := GetWhereColumns(stmt)
	for _, column := range columns {
		for _, safeColumn := range SafeColumns {
			if safeColumn.MatchString(column) {
				return true
			}
		}
	}
	golog.Info("GodEater", "CheckTable", strings.Join(tables, ","), id)
	golog.Info("GodEater", "CheckColumns", strings.Join(columns, ","), id)
	return false
}

func GetTables(stmt sqlparser.Statement) []string {
	tables := []string{}
	switch v := stmt.(type) {
	case *sqlparser.Select:
		switch te := v.From[0].(type) {
		case *sqlparser.AliasedTableExpr:
			if t, ok := te.Expr.(*sqlparser.TableName); ok {
				tables = append(tables, string(t.Name))
			}
		case *sqlparser.JoinTableExpr:
			tables = append(tables, ExtractTables(te)...)
		}
	}
	return tables
}

func ExtractTables(te *sqlparser.JoinTableExpr) []string {
	tables := []string{}
	switch le := te.LeftExpr.(type) {
	case *sqlparser.AliasedTableExpr:
		tables = append(tables, string(le.Expr.(*sqlparser.TableName).Name))
	case *sqlparser.JoinTableExpr:
		tables = append(tables, ExtractTables(le)...)
	}

	if te.Join != "join" {
		return tables
	}
	switch re := te.RightExpr.(type) {
	case *sqlparser.AliasedTableExpr:
		tables = append(tables, string(re.Expr.(*sqlparser.TableName).Name))
	case *sqlparser.JoinTableExpr:
		tables = append(tables, ExtractTables(re)...)
	}

	return tables
}

func GetWhereColumns(stmt sqlparser.Statement) []string {
	columns := []string{}
	switch v := stmt.(type) {
	case *sqlparser.Select:
		if v.Where == nil {
			return columns
		}
		switch vt := v.Where.Expr.(type) {
		case *sqlparser.AndExpr:
			columns = append(columns, ExtractColumns(vt)...)
		case *sqlparser.ComparisonExpr:
			if c, ok := IsColumn(vt.Left); ok {
				if IsValue(vt.Right) {
					columns = append(columns, c)
				}
			}
			if c, ok := IsColumn(vt.Right); ok {
				if IsValue(vt.Left) {
					columns = append(columns, c)
				}
			}
		case *sqlparser.ParenBoolExpr:
			columns = append(columns, ExtractParenColumns(vt)...)
		}
	}

	return columns
}

func ExtractColumns(ae *sqlparser.AndExpr) []string {
	columns := []string{}

	switch v := ae.Left.(type) {
	case *sqlparser.AndExpr:
		columns = append(columns, ExtractColumns(v)...)
	case *sqlparser.ComparisonExpr:
		if c, ok := IsColumn(v.Left); ok {
			if IsValue(v.Right) {
				columns = append(columns, c)
			}
		}
		if c, ok := IsColumn(v.Right); ok {
			if IsValue(v.Left) {
				columns = append(columns, c)
			}
		}
	case *sqlparser.ParenBoolExpr:
		columns = append(columns, ExtractParenColumns(v)...)
	}

	switch v := ae.Right.(type) {
	case *sqlparser.AndExpr:

		columns = append(columns, ExtractColumns(v)...)
	case *sqlparser.ComparisonExpr:
		if c, ok := IsColumn(v.Left); ok {
			if IsValue(v.Right) {
				columns = append(columns, c)
			}
		}
		if c, ok := IsColumn(v.Right); ok {
			if IsValue(v.Left) {
				columns = append(columns, c)
			}
		}
	case *sqlparser.ParenBoolExpr:
		columns = append(columns, ExtractParenColumns(v)...)
	}

	return columns
}

func ExtractParenColumns(v *sqlparser.ParenBoolExpr) []string {
	columns := []string{}
	switch t := v.Expr.(type) {
	case *sqlparser.ParenBoolExpr:
		return ExtractParenColumns(t)
	case *sqlparser.AndExpr:
		columns = append(columns, ExtractColumns(t)...)
	case *sqlparser.ComparisonExpr:
		if c, ok := IsColumn(t.Left); ok {
			if IsValue(t.Right) {
				columns = append(columns, c)
			}
		}
		if c, ok := IsColumn(t.Right); ok {
			if IsValue(t.Left) {
				columns = append(columns, c)
			}
		}
	}
	return columns
}

func IsColumn(v sqlparser.ValExpr) (string, bool) {
	if ve, ok := v.(*sqlparser.ColName); ok {
		return string(ve.Qualifier) + "." + string(ve.Name), ok
	}
	return "", false
}

func IsValue(v sqlparser.ValExpr) bool {
	switch vt := v.(type) {
	case sqlparser.NumVal:
		return true
	case sqlparser.StrVal:
		return true
	case sqlparser.ValTuple:
		return len(vt) > 0 && IsValue(vt[0])
	}
	return false
}
