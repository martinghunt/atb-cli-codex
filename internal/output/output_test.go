package output

import (
	"strings"
	"testing"

	"github.com/martin/atb-cli-codex/internal/model"
)

func TestWriteRowsFormats(t *testing.T) {
	rows := []map[string]any{{"sample_id": "S1", "species": "Escherichia coli", "hq": true}}
	for _, format := range []model.OutputFormat{model.FormatTable, model.FormatCSV, model.FormatJSON} {
		got, err := FormatRows(format, rows)
		if err != nil {
			t.Fatalf("FormatRows(%s) returned error: %v", format, err)
		}
		if !strings.Contains(got, "S1") {
			t.Fatalf("FormatRows(%s) missing row data: %q", format, got)
		}
	}
}
