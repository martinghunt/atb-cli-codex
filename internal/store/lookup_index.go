package store

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/martin/atb-cli-codex/internal/cache"
	"github.com/martin/atb-cli-codex/internal/model"
	"github.com/parquet-go/parquet-go"
	_ "modernc.org/sqlite"
)

type lookupRow struct {
	SampleID                string
	RunAccession            sql.NullString
	AssemblyAccession       sql.NullString
	AssemblySeqkitSum       sql.NullString
	ASMPipeFilter           sql.NullString
	ASMFASTAOnOSF           sql.NullInt64
	Dataset                 sql.NullString
	Species                 sql.NullString
	ScientificName          sql.NullString
	SylphSpecies            sql.NullString
	SylphSpeciesPre202505   sql.NullString
	SylphFilter             sql.NullString
	HQFilter                sql.NullString
	InHQPre202505           sql.NullString
	HQ                      bool
	OSFTarballFilename      sql.NullString
	OSFTarballURL           sql.NullString
	AWSURL                  sql.NullString
	Comments                sql.NullString
	MetadataVersion         sql.NullString
	CheckM2Completeness     sql.NullFloat64
	CheckM2Contamination    sql.NullFloat64
	AssemblyStatsTotal      sql.NullInt64
	AssemblyStatsNumber     sql.NullInt64
	AssemblyStatsMeanLength sql.NullFloat64
	AssemblyStatsLongest    sql.NullInt64
	AssemblyStatsShortest   sql.NullInt64
	AssemblyStatsNCount     sql.NullInt64
	AssemblyStatsGaps       sql.NullInt64
	AssemblyStatsN50        sql.NullInt64
	AssemblyStatsN50n       sql.NullInt64
	AssemblyStatsN70        sql.NullInt64
	AssemblyStatsN70n       sql.NullInt64
	AssemblyStatsN90        sql.NullInt64
	AssemblyStatsN90n       sql.NullInt64
}

func ensureLookupIndex(layout cache.Layout) error {
	if err := layout.Ensure(); err != nil {
		return err
	}
	needsBuild, err := lookupIndexNeedsBuild(layout)
	if err != nil {
		return err
	}
	if !needsBuild {
		return nil
	}
	return rebuildLookupIndex(layout)
}

func lookupIndexNeedsBuild(layout cache.Layout) (bool, error) {
	dbInfo, err := os.Stat(layout.LookupDB)
	if err != nil {
		if os.IsNotExist(err) {
			return true, nil
		}
		return false, fmt.Errorf("stat lookup index: %w", err)
	}
	for _, path := range lookupIndexSourcePaths(layout) {
		info, err := os.Stat(path)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return false, fmt.Errorf("stat lookup source %s: %w", path, err)
		}
		if info.ModTime().After(dbInfo.ModTime()) {
			return true, nil
		}
	}
	return false, nil
}

func lookupIndexSourcePaths(layout cache.Layout) []string {
	return []string{
		filepath.Join(layout.Metadata, "assembly.parquet"),
		filepath.Join(layout.Metadata, "checkm2.parquet"),
		filepath.Join(layout.Metadata, "assembly_stats.parquet"),
	}
}

func rebuildLookupIndex(layout cache.Layout) error {
	assemblyPath, err := firstExisting(filepath.Join(layout.Metadata, "assembly.parquet"))
	if err != nil {
		return err
	}
	assemblyRows, err := parquet.ReadFile[assemblyInfoRow](assemblyPath)
	if err != nil {
		return fmt.Errorf("read assembly parquet: %w", err)
	}
	checkBySample, err := readCheckM2BySample(layout)
	if err != nil {
		return err
	}
	statsBySample, err := readAssemblyStatsBySample(layout)
	if err != nil {
		return err
	}

	tmpPath := layout.LookupDB + ".tmp"
	_ = os.Remove(tmpPath)
	db, err := sql.Open("sqlite", tmpPath)
	if err != nil {
		return fmt.Errorf("open lookup index: %w", err)
	}
	defer db.Close()
	if _, err := db.Exec(lookupSchemaSQL); err != nil {
		return fmt.Errorf("create lookup schema: %w", err)
	}
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("begin lookup transaction: %w", err)
	}
	stmt, err := tx.Prepare(insertLookupSQL)
	if err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("prepare lookup insert: %w", err)
	}
	defer stmt.Close()

	for _, row := range assemblyRows {
		check := checkBySample[row.SampleAccession]
		stats := statsBySample[row.SampleAccession]
		hq := strings.EqualFold(row.HQFilter, "pass") || strings.EqualFold(row.HQFilter, "hq") || (check.Completeness >= 90 && check.Contamination <= 5)
		species := firstNonEmpty(row.ScientificName, row.SylphSpecies, row.SylphSpeciesPre2025)
		if _, err := stmt.Exec(
			row.SampleAccession,
			nullString(row.RunAccession),
			nullString(row.AssemblyAccession),
			nullString(row.AssemblySeqkitSum),
			nullString(row.ASMPipeFilter),
			nullInt64(row.ASMFASTAOnOSF),
			nullString(row.Dataset),
			nullString(species),
			nullString(row.ScientificName),
			nullString(row.SylphSpecies),
			nullString(row.SylphSpeciesPre2025),
			nullString(row.SylphFilter),
			nullString(row.HQFilter),
			nullString(row.InHQPre2025),
			boolToInt(hq),
			nullString(row.OSFTarballFilename),
			nullString(row.OSFTarballURL),
			nullString(row.AWSURL),
			nullString(row.Comments),
			nullString(filepath.Base(assemblyPath)),
			nullFloat64(check.Completeness),
			nullFloat64(check.Contamination),
			nullInt64(stats.TotalLength),
			nullInt64(stats.Number),
			nullFloat64(stats.MeanLength),
			nullInt64(stats.Longest),
			nullInt64(stats.Shortest),
			nullInt64(stats.NCount),
			nullInt64(stats.Gaps),
			nullInt64(stats.N50),
			nullInt64(stats.N50n),
			nullInt64(stats.N70),
			nullInt64(stats.N70n),
			nullInt64(stats.N90),
			nullInt64(stats.N90n),
		); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("insert lookup row for %s: %w", row.SampleAccession, err)
		}
	}
	if err := stmt.Close(); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("close lookup statement: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit lookup transaction: %w", err)
	}
	if err := db.Close(); err != nil {
		return fmt.Errorf("close lookup database: %w", err)
	}
	if err := os.Rename(tmpPath, layout.LookupDB); err != nil {
		return fmt.Errorf("publish lookup index: %w", err)
	}
	return nil
}

func readCheckM2BySample(layout cache.Layout) (map[string]checkm2Row, error) {
	path, err := firstExisting(filepath.Join(layout.Metadata, "checkm2*.parquet"))
	if err != nil {
		if os.IsNotExist(err) {
			return map[string]checkm2Row{}, nil
		}
		return nil, err
	}
	rows, err := parquet.ReadFile[checkm2Row](path)
	if err != nil {
		return nil, fmt.Errorf("read checkm2 parquet: %w", err)
	}
	out := make(map[string]checkm2Row, len(rows))
	for _, row := range rows {
		out[row.SampleAccession] = row
	}
	return out, nil
}

func readAssemblyStatsBySample(layout cache.Layout) (map[string]assemblyStatsRow, error) {
	path, err := firstExisting(filepath.Join(layout.Metadata, "assembly_stats.parquet"))
	if err != nil {
		if os.IsNotExist(err) {
			return map[string]assemblyStatsRow{}, nil
		}
		return nil, err
	}
	rows, err := parquet.ReadFile[assemblyStatsRow](path)
	if err != nil {
		return nil, fmt.Errorf("read assembly stats parquet: %w", err)
	}
	out := make(map[string]assemblyStatsRow, len(rows))
	for _, row := range rows {
		out[row.SampleAccession] = row
	}
	return out, nil
}

func lookupRowByID(ctx context.Context, layout cache.Layout, id string) (*lookupRow, error) {
	if err := ensureLookupIndex(layout); err != nil {
		return nil, err
	}
	db, err := sql.Open("sqlite", layout.LookupDB)
	if err != nil {
		return nil, fmt.Errorf("open lookup index: %w", err)
	}
	defer db.Close()

	var row lookupRow
	err = db.QueryRowContext(ctx, selectLookupSQL, id, id, id).Scan(
		&row.SampleID,
		&row.RunAccession,
		&row.AssemblyAccession,
		&row.AssemblySeqkitSum,
		&row.ASMPipeFilter,
		&row.ASMFASTAOnOSF,
		&row.Dataset,
		&row.Species,
		&row.ScientificName,
		&row.SylphSpecies,
		&row.SylphSpeciesPre202505,
		&row.SylphFilter,
		&row.HQFilter,
		&row.InHQPre202505,
		&row.HQ,
		&row.OSFTarballFilename,
		&row.OSFTarballURL,
		&row.AWSURL,
		&row.Comments,
		&row.MetadataVersion,
		&row.CheckM2Completeness,
		&row.CheckM2Contamination,
		&row.AssemblyStatsTotal,
		&row.AssemblyStatsNumber,
		&row.AssemblyStatsMeanLength,
		&row.AssemblyStatsLongest,
		&row.AssemblyStatsShortest,
		&row.AssemblyStatsNCount,
		&row.AssemblyStatsGaps,
		&row.AssemblyStatsN50,
		&row.AssemblyStatsN50n,
		&row.AssemblyStatsN70,
		&row.AssemblyStatsN70n,
		&row.AssemblyStatsN90,
		&row.AssemblyStatsN90n,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("no record found for %q", id)
		}
		return nil, fmt.Errorf("query lookup index: %w", err)
	}
	return &row, nil
}

func queryLookupRows(ctx context.Context, layout cache.Layout, q model.Query) ([]lookupRow, error) {
	if q.SequenceType != nil {
		return nil, ErrQueryUnsupported
	}
	if err := ensureLookupIndex(layout); err != nil {
		return nil, err
	}
	db, err := sql.Open("sqlite", layout.LookupDB)
	if err != nil {
		return nil, fmt.Errorf("open lookup index: %w", err)
	}
	defer db.Close()

	where, args := lookupQueryWhere(q)
	rows, err := db.QueryContext(ctx, selectLookupBaseSQL+where+" ORDER BY species, sample_id", args...)
	if err != nil {
		return nil, fmt.Errorf("query lookup index: %w", err)
	}
	defer rows.Close()

	var out []lookupRow
	for rows.Next() {
		var row lookupRow
		if err := rows.Scan(
			&row.SampleID,
			&row.RunAccession,
			&row.AssemblyAccession,
			&row.AssemblySeqkitSum,
			&row.ASMPipeFilter,
			&row.ASMFASTAOnOSF,
			&row.Dataset,
			&row.Species,
			&row.ScientificName,
			&row.SylphSpecies,
			&row.SylphSpeciesPre202505,
			&row.SylphFilter,
			&row.HQFilter,
			&row.InHQPre202505,
			&row.HQ,
			&row.OSFTarballFilename,
			&row.OSFTarballURL,
			&row.AWSURL,
			&row.Comments,
			&row.MetadataVersion,
			&row.CheckM2Completeness,
			&row.CheckM2Contamination,
			&row.AssemblyStatsTotal,
			&row.AssemblyStatsNumber,
			&row.AssemblyStatsMeanLength,
			&row.AssemblyStatsLongest,
			&row.AssemblyStatsShortest,
			&row.AssemblyStatsNCount,
			&row.AssemblyStatsGaps,
			&row.AssemblyStatsN50,
			&row.AssemblyStatsN50n,
			&row.AssemblyStatsN70,
			&row.AssemblyStatsN70n,
			&row.AssemblyStatsN90,
			&row.AssemblyStatsN90n,
		); err != nil {
			return nil, fmt.Errorf("scan lookup row: %w", err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate lookup rows: %w", err)
	}
	return out, nil
}

func lookupQueryWhere(q model.Query) (string, []any) {
	var clauses []string
	var args []any
	if q.Species != "" {
		clauses = append(clauses, "lower(species) = lower(?)")
		args = append(args, q.Species)
	}
	if q.HQOnly {
		clauses = append(clauses, "hq = 1")
	}
	if q.CheckM2Min != nil {
		clauses = append(clauses, "checkm2_completeness >= ?")
		args = append(args, *q.CheckM2Min)
	}
	if q.CheckM2MaxContamination != nil {
		clauses = append(clauses, "checkm2_contamination <= ?")
		args = append(args, *q.CheckM2MaxContamination)
	}
	if len(clauses) == 0 {
		return "", args
	}
	return " WHERE " + strings.Join(clauses, " AND "), args
}

func (r lookupRow) toRecord() model.Record {
	record := model.Record{
		SampleID:        r.SampleID,
		GenomeID:        firstNonEmpty(r.RunAccession.String, r.AssemblyAccession.String),
		Species:         r.Species.String,
		Genus:           genusFromSpecies(r.Species.String),
		HQ:              r.HQ,
		MetadataVersion: r.MetadataVersion.String,
	}
	if r.CheckM2Completeness.Valid {
		record.CheckM2Completeness = r.CheckM2Completeness.Float64
	}
	if r.CheckM2Contamination.Valid {
		record.CheckM2Contamination = r.CheckM2Contamination.Float64
	}
	return record
}

func (r lookupRow) toInfoRow() map[string]any {
	row := map[string]any{
		"sample_id":                r.SampleID,
		"run_accession":            r.RunAccession.String,
		"assembly_accession":       r.AssemblyAccession.String,
		"assembly_seqkit_sum":      r.AssemblySeqkitSum.String,
		"asm_pipe_filter":          r.ASMPipeFilter.String,
		"dataset":                  r.Dataset.String,
		"species":                  r.Species.String,
		"scientific_name":          r.ScientificName.String,
		"sylph_species":            r.SylphSpecies.String,
		"sylph_species_pre_202505": r.SylphSpeciesPre202505.String,
		"sylph_filter":             r.SylphFilter.String,
		"hq_filter":                r.HQFilter.String,
		"in_hq_pre_202505":         r.InHQPre202505.String,
		"hq":                       r.HQ,
		"osf_tarball_filename":     r.OSFTarballFilename.String,
		"osf_tarball_url":          r.OSFTarballURL.String,
		"aws_url":                  r.AWSURL.String,
		"comments":                 r.Comments.String,
		"metadata_version":         r.MetadataVersion.String,
	}
	if r.ASMFASTAOnOSF.Valid {
		row["asm_fasta_on_osf"] = r.ASMFASTAOnOSF.Int64
	}
	if r.CheckM2Completeness.Valid {
		row["checkm2_completeness"] = r.CheckM2Completeness.Float64
	}
	if r.CheckM2Contamination.Valid {
		row["checkm2_contamination"] = r.CheckM2Contamination.Float64
	}
	if r.AssemblyStatsTotal.Valid {
		row["total_length"] = r.AssemblyStatsTotal.Int64
	}
	if r.AssemblyStatsNumber.Valid {
		row["number"] = r.AssemblyStatsNumber.Int64
	}
	if r.AssemblyStatsMeanLength.Valid {
		row["mean_length"] = r.AssemblyStatsMeanLength.Float64
	}
	if r.AssemblyStatsLongest.Valid {
		row["longest"] = r.AssemblyStatsLongest.Int64
	}
	if r.AssemblyStatsShortest.Valid {
		row["shortest"] = r.AssemblyStatsShortest.Int64
	}
	if r.AssemblyStatsNCount.Valid {
		row["n_count"] = r.AssemblyStatsNCount.Int64
	}
	if r.AssemblyStatsGaps.Valid {
		row["gaps"] = r.AssemblyStatsGaps.Int64
	}
	if r.AssemblyStatsN50.Valid {
		row["n50"] = r.AssemblyStatsN50.Int64
	}
	if r.AssemblyStatsN50n.Valid {
		row["n50n"] = r.AssemblyStatsN50n.Int64
	}
	if r.AssemblyStatsN70.Valid {
		row["n70"] = r.AssemblyStatsN70.Int64
	}
	if r.AssemblyStatsN70n.Valid {
		row["n70n"] = r.AssemblyStatsN70n.Int64
	}
	if r.AssemblyStatsN90.Valid {
		row["n90"] = r.AssemblyStatsN90.Int64
	}
	if r.AssemblyStatsN90n.Valid {
		row["n90n"] = r.AssemblyStatsN90n.Int64
	}
	return row
}

func nullString(v string) any {
	if v == "" {
		return nil
	}
	return v
}

func nullInt64(v int64) any {
	if v == 0 {
		return nil
	}
	return v
}

func nullFloat64(v float64) any {
	if v == 0 {
		return nil
	}
	return v
}

func boolToInt(v bool) int {
	if v {
		return 1
	}
	return 0
}

const lookupSchemaSQL = `
CREATE TABLE lookup_sample (
  sample_id TEXT PRIMARY KEY,
  run_accession TEXT,
  assembly_accession TEXT,
  assembly_seqkit_sum TEXT,
  asm_pipe_filter TEXT,
  asm_fasta_on_osf INTEGER,
  dataset TEXT,
  species TEXT,
  scientific_name TEXT,
  sylph_species TEXT,
  sylph_species_pre_202505 TEXT,
  sylph_filter TEXT,
  hq_filter TEXT,
  in_hq_pre_202505 TEXT,
  hq INTEGER NOT NULL,
  osf_tarball_filename TEXT,
  osf_tarball_url TEXT,
  aws_url TEXT,
  comments TEXT,
  metadata_version TEXT,
  checkm2_completeness REAL,
  checkm2_contamination REAL,
  total_length INTEGER,
  number INTEGER,
  mean_length REAL,
  longest INTEGER,
  shortest INTEGER,
  n_count INTEGER,
  gaps INTEGER,
  n50 INTEGER,
  n50n INTEGER,
  n70 INTEGER,
  n70n INTEGER,
  n90 INTEGER,
  n90n INTEGER
);
CREATE INDEX lookup_sample_run_idx ON lookup_sample(run_accession);
CREATE INDEX lookup_sample_assembly_idx ON lookup_sample(assembly_accession);
`

const insertLookupSQL = `
INSERT INTO lookup_sample (
  sample_id, run_accession, assembly_accession, assembly_seqkit_sum, asm_pipe_filter,
  asm_fasta_on_osf, dataset, species, scientific_name, sylph_species, sylph_species_pre_202505,
  sylph_filter, hq_filter, in_hq_pre_202505, hq, osf_tarball_filename, osf_tarball_url, aws_url,
  comments, metadata_version, checkm2_completeness, checkm2_contamination, total_length, number,
  mean_length, longest, shortest, n_count, gaps, n50, n50n, n70, n70n, n90, n90n
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
`

const selectLookupSQL = `
SELECT
  sample_id, run_accession, assembly_accession, assembly_seqkit_sum, asm_pipe_filter,
  asm_fasta_on_osf, dataset, species, scientific_name, sylph_species, sylph_species_pre_202505,
  sylph_filter, hq_filter, in_hq_pre_202505, hq, osf_tarball_filename, osf_tarball_url, aws_url,
  comments, metadata_version, checkm2_completeness, checkm2_contamination, total_length, number,
  mean_length, longest, shortest, n_count, gaps, n50, n50n, n70, n70n, n90, n90n
FROM lookup_sample
WHERE sample_id = ? OR run_accession = ? OR assembly_accession = ?
LIMIT 1
`

const selectLookupBaseSQL = `
SELECT
  sample_id, run_accession, assembly_accession, assembly_seqkit_sum, asm_pipe_filter,
  asm_fasta_on_osf, dataset, species, scientific_name, sylph_species, sylph_species_pre_202505,
  sylph_filter, hq_filter, in_hq_pre_202505, hq, osf_tarball_filename, osf_tarball_url, aws_url,
  comments, metadata_version, checkm2_completeness, checkm2_contamination, total_length, number,
  mean_length, longest, shortest, n_count, gaps, n50, n50n, n70, n70n, n90, n90n
FROM lookup_sample
`
