package gox_test

import (
	"fmt"
	"testing"

	"github.com/andreyvit/edb/evm"
	"github.com/andreyvit/edb/gox"
	"github.com/andreyvit/edb/kvo"
)

func TestFieldInitialismSelectors(t *testing.T) {
	for _, tt := range []struct{ field, selector string }{
		{"AppIDs", "app_ids"},
		{"IDs", "ids"},
		{"URLs", "urls"},
		{"CustomerUUIDs", "customer_uuids"},
		{"APIURLs", "apiurls"},
		{"HTTPStatus", "http_status"},
		{"HTTPServer", "http_server"},
		{"IDsByURL", "ids_by_url"},
		{"URLsEnabled", "urls_enabled"},
		{"JSONData", "json_data"},
		{"FirstName", "first_name"},
	} {
		t.Run(tt.field, func(t *testing.T) {
			declaration := fmt.Sprintf("type Integration struct { %s string }\n", tt.field)
			source := declaration + fmt.Sprintf("func Page(b *gox.Builder) {\n integration := Integration{%s: \"ok\"}\n <p>{integration.%s}</p>\n}\n", tt.field, tt.selector)
			if got := render(t, source); got != "<p>ok</p>" {
				t.Fatal(got)
			}
		})
	}
}

func TestFieldSelectorsPreserveExplicitNames(t *testing.T) {
	source := "type Integration struct { AppIDs string `json:\"external_ids,omitempty\"` }\n" + `
func Page(b *gox.Builder) {
 integration := Integration{AppIDs: "ok"}
 <p>{integration.AppIDs}{integration.app_ids}{integration.external_ids}</p>
}

`
	if got := render(t, source); got != "<p>okokok</p>" {
		t.Fatal(got)
	}
	_, err := gox.Compile("broken_alias.gox", `type Integration struct { AppIDs string }
func Page(b *gox.Builder) {
 integration := Integration{AppIDs: "ok"}
 <p>{integration.app_i_ds}</p>
}`)
	if err == nil {
		t.Fatal("accepted the obsolete incorrectly split initialism")
	}
}

func TestKVOSelectorsAreAuthoritative(t *testing.T) {
	schema := kvo.NewSchema()
	key := kvo.NewProp(schema, 1, "customer:AppIDs", kvo.TMachineString, nil)
	model := kvo.NewModel(schema, kvo.NewEntityType(schema, "customer"), func(b *kvo.ModelBuilder) {
		b.Prop(schema.MustPropByCode(key), kvo.PropName("ids"))
	})
	pkg := evm.Package{Path: "db", Records: []*evm.RecordType{evm.NewRecordType("Customer", model, false)}}
	for _, name := range []string{"ids", "AppIDs", "customer_AppIDs", "app_ids", "customer_app_ids"} {
		t.Run(name, func(t *testing.T) {
			source := fmt.Sprintf("func Page(b *gox.Builder, customer *db.Customer) {\n <p>{customer.%s}</p>\n}", name)
			_, err := gox.Compile("record.gox", source, pkg)
			if want := model.PropBySelector(name) != nil; (err == nil) != want {
				t.Fatalf("GoX selector %q: error %v, model permits selector: %v", name, err, want)
			}
		})
	}
}
