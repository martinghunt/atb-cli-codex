package store

import (
	"compress/gzip"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/martin/atb-cli-codex/internal/cache"
	"github.com/martin/atb-cli-codex/internal/model"
	"github.com/parquet-go/parquet-go"
)

var ErrCacheNotFetched = errors.New("local ATB cache is missing required data")

type Store interface {
	Records(context.Context) ([]model.Record, error)
	RecordByID(context.Context, string) (model.Record, error)
	InfoRow(context.Context, string, bool) (map[string]any, error)
	AMRByGenus(context.Context, string) ([]model.AMRHit, error)
	Assemblies(context.Context) ([]model.AssemblyEntry, error)
	State(context.Context) (model.State, error)
}

type LocalStore struct {
	Layout cache.Layout
}

func (s LocalStore) Records(_ context.Context) ([]model.Record, error) {
	var records []model.Record
	if err := readJSON(filepath.Join(s.Layout.Metadata, "records.json"), &records); err == nil {
		slices.SortFunc(records, func(a, b model.Record) int {
			if c := strings.Compare(a.Species, b.Species); c != 0 {
				return c
			}
			return strings.Compare(a.SampleID, b.SampleID)
		})
		return records, nil
	}
	records, err := s.recordsFromParquet()
	if err != nil {
		return nil, wrapCacheErr("metadata query index", err)
	}
	slices.SortFunc(records, func(a, b model.Record) int {
		if c := strings.Compare(a.Species, b.Species); c != 0 {
			return c
		}
		return strings.Compare(a.SampleID, b.SampleID)
	})
	return records, nil
}

func (s LocalStore) RecordByID(ctx context.Context, id string) (model.Record, error) {
	var records []model.Record
	if err := readJSON(filepath.Join(s.Layout.Metadata, "records.json"), &records); err == nil {
		for _, record := range records {
			if record.SampleID == id || record.GenomeID == id {
				return record, nil
			}
		}
		return model.Record{}, fmt.Errorf("no record found for %q", id)
	}
	if row, err := lookupRowByID(ctx, s.Layout, id); err == nil {
		return row.toRecord(), nil
	}
	record, err := s.recordFromAssembly(id)
	if err == nil {
		return record, nil
	}
	if !os.IsNotExist(err) && !strings.Contains(err.Error(), "no record found") {
		return model.Record{}, wrapCacheErr("assembly metadata", err)
	}
	records, err = s.Records(ctx)
	if err != nil {
		return model.Record{}, err
	}
	for _, record := range records {
		if record.SampleID == id || record.GenomeID == id {
			return record, nil
		}
	}
	return model.Record{}, fmt.Errorf("no record found for %q", id)
}

func (s LocalStore) InfoRow(ctx context.Context, id string, includeENA bool) (map[string]any, error) {
	if row, err := lookupRowByID(ctx, s.Layout, id); err == nil {
		info := row.toInfoRow()
		if includeENA {
			sampleID := fmt.Sprint(info["sample_id"])
			if ena, err := s.lookupENA(sampleID, id); err == nil {
				for k, v := range ena {
					info[k] = v
				}
			}
		}
		return info, nil
	}
	row, err := s.lookupAssemblyInfo(id)
	if err != nil {
		return nil, wrapCacheErr("assembly metadata", err)
	}
	sampleID := fmt.Sprint(row["sample_id"])
	if check, err := s.lookupCheckM2(sampleID); err == nil {
		for k, v := range check {
			row[k] = v
		}
	}
	if stats, err := s.lookupAssemblyStats(sampleID); err == nil {
		for k, v := range stats {
			row[k] = v
		}
	}
	if includeENA {
		if ena, err := s.lookupENA(sampleID, id); err == nil {
			for k, v := range ena {
				row[k] = v
			}
		}
	}
	return row, nil
}

func (s LocalStore) AMRByGenus(_ context.Context, genus string) ([]model.AMRHit, error) {
	var hits []model.AMRHit
	name := fmt.Sprintf("%s.json", strings.ToLower(genus))
	if err := readJSON(filepath.Join(s.Layout.AMR, name), &hits); err == nil {
		slices.SortFunc(hits, func(a, b model.AMRHit) int {
			if c := strings.Compare(a.SampleID, b.SampleID); c != 0 {
				return c
			}
			return strings.Compare(a.GeneSymbol, b.GeneSymbol)
		})
		return hits, nil
	}
	hits, err := s.amrFromParquet(genus)
	if err != nil {
		return nil, wrapCacheErr("AMR partition", err)
	}
	slices.SortFunc(hits, func(a, b model.AMRHit) int {
		if c := strings.Compare(a.SampleID, b.SampleID); c != 0 {
			return c
		}
		return strings.Compare(a.GeneSymbol, b.GeneSymbol)
	})
	return hits, nil
}

func (s LocalStore) Assemblies(_ context.Context) ([]model.AssemblyEntry, error) {
	var entries []model.AssemblyEntry
	if err := readJSON(filepath.Join(s.Layout.Manifests, "assemblies.json"), &entries); err == nil {
		return entries, nil
	}
	entries, err := s.assembliesFromTSV()
	if err != nil {
		return nil, wrapCacheErr("assembly manifest", err)
	}
	return entries, nil
}

func (s LocalStore) State(_ context.Context) (model.State, error) {
	return s.Layout.ReadState()
}

func readJSON(path string, out any) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(data, out); err != nil {
		return fmt.Errorf("decode %s: %w", path, err)
	}
	return nil
}

func wrapCacheErr(name string, err error) error {
	if os.IsNotExist(err) {
		return fmt.Errorf("%w: %s not found. Run `atb fetch --metadata` first or point --cache-dir at an existing cache", ErrCacheNotFetched, name)
	}
	return err
}

type enaRow struct {
	SampleAccession string `parquet:"sample_accession"`
	RunAccession    string `parquet:"run_accession,optional"`
	ScientificName  string `parquet:"scientific_name,optional"`
	Country         string `parquet:"country,optional"`
}

type checkm2Row struct {
	SampleAccession string  `parquet:"sample_accession"`
	Completeness    float64 `parquet:"Completeness_General,optional"`
	Contamination   float64 `parquet:"Contamination,optional"`
}

type amrRow struct {
	Name    string `parquet:"Name"`
	Gene    string `parquet:"Gene symbol,optional"`
	Species string `parquet:"Species,optional"`
	Class   string `parquet:"Class,optional"`
	Method  string `parquet:"Method,optional"`
}

type assemblyInfoRow struct {
	SampleAccession     string `parquet:"sample_accession"`
	RunAccession        string `parquet:"run_accession,optional"`
	AssemblyAccession   string `parquet:"assembly_accession,optional"`
	AssemblySeqkitSum   string `parquet:"assembly_seqkit_sum,optional"`
	ASMPipeFilter       string `parquet:"asm_pipe_filter,optional"`
	ASMFASTAOnOSF       int64  `parquet:"asm_fasta_on_osf,optional"`
	Dataset             string `parquet:"dataset,optional"`
	ScientificName      string `parquet:"scientific_name,optional"`
	SylphSpeciesPre2025 string `parquet:"sylph_species_pre_202505,optional"`
	InHQPre2025         string `parquet:"in_hq_pre_202505,optional"`
	SylphSpecies        string `parquet:"sylph_species,optional"`
	SylphFilter         string `parquet:"sylph_filter,optional"`
	HQFilter            string `parquet:"hq_filter,optional"`
	OSFTarballFilename  string `parquet:"osf_tarball_filename,optional"`
	OSFTarballURL       string `parquet:"osf_tarball_url,optional"`
	AWSURL              string `parquet:"aws_url,optional"`
	Comments            string `parquet:"comments,optional"`
}

type assemblyStatsRow struct {
	SampleAccession string  `parquet:"sample_accession"`
	TotalLength     int64   `parquet:"total_length,optional"`
	Number          int64   `parquet:"number,optional"`
	MeanLength      float64 `parquet:"mean_length,optional"`
	Longest         int64   `parquet:"longest,optional"`
	Shortest        int64   `parquet:"shortest,optional"`
	NCount          int64   `parquet:"N_count,optional"`
	Gaps            int64   `parquet:"Gaps,optional"`
	N50             int64   `parquet:"N50,optional"`
	N50n            int64   `parquet:"N50n,optional"`
	N70             int64   `parquet:"N70,optional"`
	N70n            int64   `parquet:"N70n,optional"`
	N90             int64   `parquet:"N90,optional"`
	N90n            int64   `parquet:"N90n,optional"`
}

func (s LocalStore) recordsFromParquet() ([]model.Record, error) {
	enaPath, err := firstExisting(filepath.Join(s.Layout.Metadata, "ena_*.parquet"))
	if err != nil {
		return nil, err
	}
	enaRows, err := parquet.ReadFile[enaRow](enaPath)
	if err != nil {
		return nil, fmt.Errorf("read metadata parquet: %w", err)
	}

	checkm2BySample := map[string]checkm2Row{}
	if checkm2Path, err := firstExisting(filepath.Join(s.Layout.Metadata, "checkm2*.parquet")); err == nil {
		checkRows, err := parquet.ReadFile[checkm2Row](checkm2Path)
		if err != nil {
			return nil, fmt.Errorf("read checkm2 parquet: %w", err)
		}
		for _, row := range checkRows {
			checkm2BySample[row.SampleAccession] = row
		}
	}

	records := make([]model.Record, 0, len(enaRows))
	for _, row := range enaRows {
		record := model.Record{
			SampleID:        row.SampleAccession,
			GenomeID:        row.RunAccession,
			Species:         row.ScientificName,
			Genus:           genusFromSpecies(row.ScientificName),
			Country:         row.Country,
			MetadataVersion: filepath.Base(enaPath),
		}
		if check, ok := checkm2BySample[row.SampleAccession]; ok {
			record.CheckM2Completeness = check.Completeness
			record.CheckM2Contamination = check.Contamination
			record.HQ = check.Completeness >= 90 && check.Contamination <= 5
		}
		records = append(records, record)
	}
	return records, nil
}

func (s LocalStore) amrFromParquet(genus string) ([]model.AMRHit, error) {
	parquetPath, err := firstExisting(filepath.Join(s.Layout.AMR, strings.ToLower(genus)+"*.parquet"))
	if err != nil {
		return nil, err
	}
	rows, err := parquet.ReadFile[amrRow](parquetPath)
	if err != nil {
		return nil, fmt.Errorf("read AMR parquet: %w", err)
	}
	hits := make([]model.AMRHit, 0, len(rows))
	for _, row := range rows {
		hits = append(hits, model.AMRHit{
			SampleID:   row.Name,
			Species:    row.Species,
			DrugClass:  row.Class,
			GeneSymbol: row.Gene,
			Method:     row.Method,
			Genus:      genus,
			AMRVersion: filepath.Base(parquetPath),
		})
	}
	return hits, nil
}

func (s LocalStore) recordFromAssembly(id string) (model.Record, error) {
	row, err := s.lookupAssemblyInfo(id)
	if err != nil {
		return model.Record{}, err
	}
	record := model.Record{
		SampleID:        fmt.Sprint(row["sample_id"]),
		GenomeID:        firstNonEmpty(fmt.Sprint(row["run_accession"]), fmt.Sprint(row["assembly_accession"])),
		Species:         fmt.Sprint(row["species"]),
		Genus:           genusFromSpecies(fmt.Sprint(row["species"])),
		HQ:              row["hq"] == true,
		MetadataVersion: fmt.Sprint(row["metadata_version"]),
	}
	if check, err := s.lookupCheckM2(record.SampleID); err == nil {
		if completeness, ok := check["checkm2_completeness"].(float64); ok {
			record.CheckM2Completeness = completeness
		}
		if contamination, ok := check["checkm2_contamination"].(float64); ok {
			record.CheckM2Contamination = contamination
		}
		record.HQ = record.HQ || (record.CheckM2Completeness >= 90 && record.CheckM2Contamination <= 5)
	}
	return record, nil
}

func (s LocalStore) lookupAssemblyInfo(id string) (map[string]any, error) {
	assemblyPath, err := firstExisting(filepath.Join(s.Layout.Metadata, "assembly.parquet"))
	if err != nil {
		return nil, err
	}
	match, err := firstParquetMatch(assemblyPath, func(row assemblyInfoRow) bool {
		return row.SampleAccession == id || row.RunAccession == id || row.AssemblyAccession == id
	})
	if err != nil {
		return nil, fmt.Errorf("read assembly parquet: %w", err)
	}
	if match == nil {
		return nil, fmt.Errorf("no record found for %q", id)
	}
	row := *match
	return map[string]any{
		"sample_id":                row.SampleAccession,
		"run_accession":            row.RunAccession,
		"assembly_accession":       row.AssemblyAccession,
		"assembly_seqkit_sum":      row.AssemblySeqkitSum,
		"asm_pipe_filter":          row.ASMPipeFilter,
		"asm_fasta_on_osf":         row.ASMFASTAOnOSF,
		"dataset":                  row.Dataset,
		"species":                  firstNonEmpty(row.ScientificName, row.SylphSpecies, row.SylphSpeciesPre2025),
		"scientific_name":          row.ScientificName,
		"sylph_species":            row.SylphSpecies,
		"sylph_species_pre_202505": row.SylphSpeciesPre2025,
		"sylph_filter":             row.SylphFilter,
		"hq_filter":                row.HQFilter,
		"in_hq_pre_202505":         row.InHQPre2025,
		"hq":                       strings.EqualFold(row.HQFilter, "pass") || strings.EqualFold(row.HQFilter, "hq"),
		"osf_tarball_filename":     row.OSFTarballFilename,
		"osf_tarball_url":          row.OSFTarballURL,
		"aws_url":                  row.AWSURL,
		"comments":                 row.Comments,
		"metadata_version":         filepath.Base(assemblyPath),
	}, nil
}

func (s LocalStore) lookupCheckM2(sampleID string) (map[string]any, error) {
	checkm2Path, err := firstExisting(filepath.Join(s.Layout.Metadata, "checkm2*.parquet"))
	if err != nil {
		return nil, err
	}
	match, err := firstParquetMatch(checkm2Path, func(row checkm2Row) bool {
		return row.SampleAccession == sampleID
	})
	if err != nil {
		return nil, fmt.Errorf("read checkm2 parquet: %w", err)
	}
	if match == nil {
		return nil, os.ErrNotExist
	}
	return map[string]any{
		"checkm2_completeness":  match.Completeness,
		"checkm2_contamination": match.Contamination,
	}, nil
}

func (s LocalStore) lookupAssemblyStats(sampleID string) (map[string]any, error) {
	statsPath, err := firstExisting(filepath.Join(s.Layout.Metadata, "assembly_stats.parquet"))
	if err != nil {
		return nil, err
	}
	match, err := firstParquetMatch(statsPath, func(row assemblyStatsRow) bool {
		return row.SampleAccession == sampleID
	})
	if err != nil {
		return nil, fmt.Errorf("read assembly stats parquet: %w", err)
	}
	if match == nil {
		return nil, os.ErrNotExist
	}
	return map[string]any{
		"total_length": match.TotalLength,
		"number":       match.Number,
		"mean_length":  match.MeanLength,
		"longest":      match.Longest,
		"shortest":     match.Shortest,
		"n_count":      match.NCount,
		"gaps":         match.Gaps,
		"n50":          match.N50,
		"n50n":         match.N50n,
		"n70":          match.N70,
		"n70n":         match.N70n,
		"n90":          match.N90,
		"n90n":         match.N90n,
	}, nil
}

func (s LocalStore) lookupENA(sampleID, id string) (map[string]any, error) {
	enaPath, err := firstExisting(filepath.Join(s.Layout.Metadata, "ena_*.parquet"))
	if err != nil {
		return nil, err
	}
	match, err := firstParquetMatch(enaPath, func(row enaRow) bool {
		return row.SampleAccession == sampleID || row.SampleAccession == id || row.RunAccession == id
	})
	if err != nil {
		return nil, fmt.Errorf("read ENA parquet: %w", err)
	}
	if match == nil {
		return nil, os.ErrNotExist
	}
	return map[string]any{
		"ena_run_accession":   match.RunAccession,
		"ena_scientific_name": match.ScientificName,
		"country":             match.Country,
	}, nil
}

func firstExisting(pattern string) (string, error) {
	matches, err := filepath.Glob(pattern)
	if err != nil {
		return "", err
	}
	if len(matches) == 0 {
		return "", os.ErrNotExist
	}
	slices.Sort(matches)
	return matches[0], nil
}

func firstParquetMatch[T any](path string, predicate func(T) bool) (*T, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	reader := parquet.NewGenericReader[T](f)
	rows := make([]T, 256)
	for {
		n, err := reader.Read(rows)
		for i := 0; i < n; i++ {
			if predicate(rows[i]) {
				match := rows[i]
				return &match, nil
			}
		}
		if err == io.EOF {
			return nil, nil
		}
		if err != nil {
			return nil, err
		}
	}
}

func genusFromSpecies(species string) string {
	if species == "" {
		return ""
	}
	parts := strings.Fields(species)
	if len(parts) == 0 {
		return ""
	}
	return parts[0]
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func (s LocalStore) assembliesFromTSV() ([]model.AssemblyEntry, error) {
	manifestPath, err := firstExisting(filepath.Join(s.Layout.Manifests, "file_list*.tsv.gz"))
	if err != nil {
		return nil, err
	}
	f, err := os.Open(manifestPath)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	gr, err := gzip.NewReader(f)
	if err != nil {
		return nil, fmt.Errorf("open gzipped assembly manifest: %w", err)
	}
	defer gr.Close()
	reader := csv.NewReader(gr)
	reader.Comma = '\t'
	reader.FieldsPerRecord = -1
	header, err := reader.Read()
	if err != nil {
		return nil, fmt.Errorf("read assembly manifest header: %w", err)
	}
	index := map[string]int{}
	for i, column := range header {
		index[column] = i
	}
	required := []string{"sample", "filename_in_tar_xz", "tar_xz", "tar_xz_url"}
	for _, column := range required {
		if _, ok := index[column]; !ok {
			return nil, fmt.Errorf("assembly manifest is missing required column %q", column)
		}
	}
	var entries []model.AssemblyEntry
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("read assembly manifest row: %w", err)
		}
		sample := record[index["sample"]]
		entries = append(entries, model.AssemblyEntry{
			SampleID:       sample,
			TarballName:    record[index["tar_xz"]],
			TarballURL:     record[index["tar_xz_url"]],
			FileInTarball:  record[index["filename_in_tar_xz"]],
			AWSURL:         fmt.Sprintf("https://allthebacteria-assemblies.s3.amazonaws.com/%s.fa.gz", sample),
			AssemblySHA256: "",
		})
	}
	return entries, nil
}
