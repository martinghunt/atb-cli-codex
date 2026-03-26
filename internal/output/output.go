package output

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
	"text/tabwriter"

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
	rows := []map[string]any{}
	keys := make([]string, 0, len(stats.PerSpecies))
	for species := range stats.PerSpecies {
		keys = append(keys, species)
	}
	sort.Strings(keys)
	for _, species := range keys {
		rows = append(rows, map[string]any{"species": species, "count": stats.PerSpecies[species]})
	}
	if format == model.FormatJSON {
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		return enc.Encode(stats)
	}
	if _, err := fmt.Fprintf(w, "total_genomes\t%d\n", stats.Total); err != nil {
		return err
	}
	return writeTable(w, rows)
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
	tw := tabwriter.NewWriter(w, 0, 8, 2, ' ', 0)
	if len(headers) == 0 {
		_, err := fmt.Fprintln(w, "No rows.")
		return err
	}
	if _, err := fmt.Fprintln(tw, strings.Join(headers, "\t")); err != nil {
		return err
	}
	for _, row := range rows {
		values := make([]string, 0, len(headers))
		for _, header := range headers {
			values = append(values, fmt.Sprint(row[header]))
		}
		if _, err := fmt.Fprintln(tw, strings.Join(values, "\t")); err != nil {
			return err
		}
	}
	return tw.Flush()
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
