package output

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/martin/atb-cli-codex/internal/model"
)

func WriteRows(w io.Writer, format model.OutputFormat, rows []map[string]any) error {
	switch format {
	case model.FormatJSON:
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		return enc.Encode(rows)
	case model.FormatCSV, model.FormatTSV:
		return writeDelimited(w, format, rows)
	case model.FormatTable:
		return writeTable(w, rows)
	default:
		return fmt.Errorf("unsupported --format %q; valid values are table, csv, tsv, json", format)
	}
}

func WriteStats(w io.Writer, format model.OutputFormat, stats model.Stats) error {
	if format == model.FormatJSON {
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		return enc.Encode(stats)
	}
	if _, err := fmt.Fprintf(w, "metric\tvalue\n"); err != nil {
		return err
	}
	summary := []struct {
		name  string
		value any
	}{
		{"total_genomes", stats.Total},
		{"hq_genomes", stats.HQ},
		{"non_hq_genomes", stats.NonHQ},
		{"checkm2_completeness_ge_90", stats.CheckM2CompletenessGE90},
		{"checkm2_contamination_le_5", stats.CheckM2ContaminationLE5},
	}
	for _, row := range summary {
		if _, err := fmt.Fprintf(w, "%s\t%v\n", row.name, row.value); err != nil {
			return err
		}
	}
	if _, err := fmt.Fprintln(w); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(w, "section\tname\tcount"); err != nil {
		return err
	}
	for _, entry := range namedCountsRows("species", stats.PerSpecies) {
		if _, err := fmt.Fprintf(w, "species\t%s\t%d\n", entry.Name, entry.Count); err != nil {
			return err
		}
	}
	for _, entry := range namedCountsRows("genus", stats.PerGenus) {
		if _, err := fmt.Fprintf(w, "genus\t%s\t%d\n", entry.Name, entry.Count); err != nil {
			return err
		}
	}
	for _, entry := range stats.TopSpecies {
		if _, err := fmt.Fprintf(w, "top_species\t%s\t%d\n", entry.Name, entry.Count); err != nil {
			return err
		}
	}
	if _, err := fmt.Fprintln(w); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(w, "field\tpresent\ttotal\tpercentage"); err != nil {
		return err
	}
	for _, field := range stats.FieldCoverage {
		if _, err := fmt.Fprintf(w, "%s\t%d\t%d\t%d\n", field.Field, field.Present, field.Total, field.Percentage); err != nil {
			return err
		}
	}
	return nil
}

func namedCountsRows(_ string, items map[string]int) []model.NamedCount {
	out := make([]model.NamedCount, 0, len(items))
	for name, count := range items {
		out = append(out, model.NamedCount{Name: name, Count: count})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Name == out[j].Name {
			return out[i].Count < out[j].Count
		}
		return out[i].Name < out[j].Name
	})
	return out
}

func writeDelimited(w io.Writer, format model.OutputFormat, rows []map[string]any) error {
	headers := headers(rows)
	cw := csv.NewWriter(w)
	if format == model.FormatTSV {
		cw.Comma = '\t'
	}
	if err := cw.Write(headers); err != nil {
		return err
	}
	for _, row := range rows {
		record := make([]string, 0, len(headers))
		for _, header := range headers {
			record = append(record, fmt.Sprint(row[header]))
		}
		if err := cw.Write(record); err != nil {
			return err
		}
	}
	cw.Flush()
	return cw.Error()
}

func writeTable(w io.Writer, rows []map[string]any) error {
	headers := headers(rows)
	if len(headers) == 0 {
		_, err := fmt.Fprintln(w, "No rows.")
		return err
	}
	if _, err := fmt.Fprintln(w, strings.Join(headers, "\t")); err != nil {
		return err
	}
	for _, row := range rows {
		values := make([]string, 0, len(headers))
		for _, header := range headers {
			values = append(values, fmt.Sprint(row[header]))
		}
		if _, err := fmt.Fprintln(w, strings.Join(values, "\t")); err != nil {
			return err
		}
	}
	return nil
}

func headers(rows []map[string]any) []string {
	if len(rows) == 0 {
		return nil
	}
	headers := make([]string, 0, len(rows[0]))
	for header := range rows[0] {
		headers = append(headers, header)
	}
	sort.Strings(headers)
	return headers
}

func FormatRows(format model.OutputFormat, rows []map[string]any) (string, error) {
	var b bytes.Buffer
	if err := WriteRows(&b, format, rows); err != nil {
		return "", err
	}
	return b.String(), nil
}
