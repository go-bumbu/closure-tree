package closuretree_test

import (
	"context"
	"errors"
	"testing"

	closuretree "github.com/go-bumbu/closure-tree"
	"github.com/go-bumbu/testdbs"
)

type ownParentField struct {
	closuretree.Node
	Name     string
	ParentID uint // gorm maps this to column parent_id
}

type ownParentTag struct {
	closuretree.Node
	Name string
	Up   uint `gorm:"column:parent_id"`
}

func TestNewRejectsOwnParentIDColumn(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			gdb := connAndClose(t, db)
			for _, model := range []any{ownParentField{}, ownParentTag{}} {
				if _, err := closuretree.New(gdb, model); !errors.Is(err, closuretree.ErrParentIDColumn) {
					t.Errorf("%T: want ErrParentIDColumn, got %v", model, err)
				}
			}
			// models that embed Node directly and through another struct stay valid
			for _, model := range []any{TestPayload{}, MultiLevelPayload{}} {
				if _, err := closuretree.New(gdb, model); err != nil {
					t.Errorf("%T must stay valid: %v", model, err)
				}
			}
		})
	}
}

// Node's own columns must be recognised when Node is embedded through another struct.
func TestNodeFieldsOnMultiLevelModel(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			gdb := connAndClose(t, db)
			dropTreeTables(gdb, MultiLevelPayload{})
			ct, err := newTestTree(gdb, MultiLevelPayload{})
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()

			p, a, b := &MultiLevelPayload{Name: "p"}, &MultiLevelPayload{Name: "a"}, &MultiLevelPayload{Name: "b"}
			if err := ct.Add(ctx, p, nil, nil, tenant1); err != nil {
				t.Fatal(err)
			}
			for _, n := range []*MultiLevelPayload{a, b} { // b is placed first, at -10
				if err := ct.Add(ctx, n, up(p.NodeId), nil, tenant1); err != nil {
					t.Fatal(err)
				}
			}
			if err := ct.Update(ctx, b.NodeId, &MultiLevelPayload{Name: "renamed"}, nil, nil, tenant1); err != nil {
				t.Fatal(err)
			}
			var got MultiLevelPayload
			if err := ct.GetNode(ctx, b.NodeId, tenant1, &got); err != nil || got.Name != "renamed" || got.SortOrder != -10 {
				t.Errorf("Update must keep sort_order -10: name %q, sort %v, err %v", got.Name, got.SortOrder, err)
			}
			_, err = ct.FindChild(ctx, p.NodeId, tenant1, &MultiLevelPayload{}, []string{"SortOrder"}, &got)
			if !errors.Is(err, closuretree.ErrUnknownMatchField) {
				t.Errorf("a Node field as match field: want ErrUnknownMatchField, got %v", err)
			}
		})
	}
}
