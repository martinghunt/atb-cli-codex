package output

import (
	"bytes"
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

func TestTableFormatUsesLiteralTabs(t *testing.T) {
	rows := []map[string]any{{"sample_id": "S1", "species": "Escherichia coli", "hq": true}}
	got, err := FormatRows(model.FormatTable, rows)
	if err != nil {
		t.Fatalf("FormatRows(table) returned error: %v", err)
	}
	if !strings.Contains(got, "hq\tsample_id\tspecies\n") {
		t.Fatalf("expected tab-delimited header, got %q", got)
	}
	if !strings.Contains(got, "true\tS1\tEscherichia coli\n") {
		t.Fatalf("expected tab-delimited row, got %q", got)
	}
}

func TestWriteStatsExtendedTSV(t *testing.T) {
	stats := model.Stats{
		Total:                   3,
		PerSpecies:              map[string]int{"Escherichia coli": 2, "Salmonella enterica": 1},
		PerGenus:                map[string]int{"Escherichia": 2, "Salmonella": 1},
		HQ:                      2,
		NonHQ:                   1,
		CheckM2CompletenessGE90: 2,
		CheckM2ContaminationLE5: 2,
		TopSpecies:              []model.NamedCount{{Name: "Escherichia coli", Count: 2}},
		FieldCoverage:           []model.FieldCoverage{{Field: "species", Present: 3, Total: 3, Percentage: 100}},
	}
	var b bytes.Buffer
	if err := WriteStats(&b, model.FormatTSV, stats); err != nil {
		t.Fatalf("WriteStats returned error: %v", err)
	}
	got := b.String()
	for _, want := range []string{
		"metric\tvalue\n",
		"total_genomes\t3\n",
		"section\tname\tcount\n",
		"species\tEscherichia coli\t2\n",
		"genus\tEscherichia\t2\n",
		"top_species\tEscherichia coli\t2\n",
		"field\tpresent\ttotal\tpercentage\n",
		"species\t3\t3\t100\n",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("expected %q in stats output, got %q", want, got)
		}
	}
}
