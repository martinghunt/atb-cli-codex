package store

import (
	"compress/gzip"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/martin/atb-cli-codex/internal/cache"
	"github.com/martin/atb-cli-codex/internal/model"
	"github.com/parquet-go/parquet-go"
)

type enaFixtureRow struct {
	SampleAccession string `parquet:"sample_accession"`
	RunAccession    string `parquet:"run_accession,optional"`
	ScientificName  string `parquet:"scientific_name,optional"`
	Country         string `parquet:"country,optional"`
}

type checkFixtureRow struct {
	SampleAccession string  `parquet:"sample_accession"`
	Completeness    float64 `parquet:"Completeness_General,optional"`
	Contamination   float64 `parquet:"Contamination,optional"`
}

type amrFixtureRow struct {
	Name    string `parquet:"Name"`
	Gene    string `parquet:"Gene symbol,optional"`
	Species string `parquet:"Species,optional"`
	Class   string `parquet:"Class,optional"`
	Method  string `parquet:"Method,optional"`
}

func TestLocalStoreReadsRealisticParquetSchemas(t *testing.T) {
	layout := cache.NewLayout(t.TempDir())
	if err := layout.Ensure(); err != nil {
		t.Fatalf("Ensure: %v", err)
	}

	writeParquet(t, filepath.Join(layout.Metadata, "ena_202505_used.parquet"), []enaFixtureRow{
		{SampleAccession: "SAMD00000344", RunAccession: "DRR000001", ScientificName: "Escherichia coli", Country: "Japan"},
		{SampleAccession: "SAMD00000345", RunAccession: "DRR000002", ScientificName: "Salmonella enterica", Country: "UK"},
	})
	writeParquet(t, filepath.Join(layout.Metadata, "checkm2.parquet"), []checkFixtureRow{
		{SampleAccession: "SAMD00000344", Completeness: 100, Contamination: 0.66},
		{SampleAccession: "SAMD00000345", Completeness: 88, Contamination: 7.2},
	})
	writeParquet(t, filepath.Join(layout.AMR, "escherichia.parquet"), []amrFixtureRow{
		{Name: "SAMD00000344", Gene: "blaEC", Species: "Escherichia coli", Class: "BETA-LACTAM", Method: "BLASTX"},
	})

	store := LocalStore{Layout: layout}
	records, err := store.Records(context.Background())
	if err != nil {
		t.Fatalf("Records: %v", err)
	}
	if len(records) != 2 {
		t.Fatalf("expected 2 records, got %d", len(records))
	}
	if !records[0].HQ {
		t.Fatalf("expected first record to be HQ: %#v", records[0])
	}
	if records[1].HQ {
		t.Fatalf("expected second record not to be HQ: %#v", records[1])
	}
	if records[0].Genus != "Escherichia" {
		t.Fatalf("expected genus from scientific_name, got %#v", records[0])
	}

	hits, err := store.AMRByGenus(context.Background(), "escherichia")
	if err != nil {
		t.Fatalf("AMRByGenus: %v", err)
	}
	if len(hits) != 1 || hits[0].GeneSymbol != "blaEC" {
		t.Fatalf("unexpected AMR hits: %#v", hits)
	}
}

func TestLocalStoreReadsAssemblyManifestTSV(t *testing.T) {
	layout := cache.NewLayout(t.TempDir())
	if err := layout.Ensure(); err != nil {
		t.Fatalf("Ensure: %v", err)
	}
	writeGzipText(t, filepath.Join(layout.Manifests, "file_list.all.latest.tsv.gz"), stringsJoin(
		"sample\tsylph_species\tfilename_in_tar_xz\ttar_xz\ttar_xz_url\ttar_xz_md5\ttar_xz_size_MB",
		"SAMD00000344\tPaucilactobacillus hokkaidonensis\tatb.assembly.r0.2.batch.127/SAMD00000344.fa\tatb.assembly.r0.2.batch.127.tar.xz\thttps://osf.io/download/6671719165e1de5eb5893c28/\ta958b4bf1e631cfbde5424b4396645e3\t371.03",
	))
	store := LocalStore{Layout: layout}
	entries, err := store.Assemblies(context.Background())
	if err != nil {
		t.Fatalf("Assemblies: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 assembly entry, got %#v", entries)
	}
	if entries[0].AWSURL != "https://allthebacteria-assemblies.s3.amazonaws.com/SAMD00000344.fa.gz" {
		t.Fatalf("unexpected AWS URL: %#v", entries[0])
	}
	if entries[0].TarballName != "atb.assembly.r0.2.batch.127.tar.xz" {
		t.Fatalf("unexpected tarball name: %#v", entries[0])
	}
}

func TestLocalStoreInfoRowUsesAssemblyMetadataFirst(t *testing.T) {
	layout := cache.NewLayout(t.TempDir())
	if err := layout.Ensure(); err != nil {
		t.Fatalf("Ensure: %v", err)
	}
	writeParquet(t, filepath.Join(layout.Metadata, "assembly.parquet"), []assemblyInfoRow{
		{
			SampleAccession:    "SAMD00000692",
			RunAccession:       "DRR000692",
			AssemblyAccession:  "GCA_000692",
			ScientificName:     "Escherichia coli",
			SylphSpecies:       "Escherichia coli",
			HQFilter:           "pass",
			OSFTarballFilename: "batch.tar.xz",
			OSFTarballURL:      "https://example.org/batch.tar.xz",
			AWSURL:             "https://example.org/SAMD00000692.fa.gz",
		},
	})
	writeParquet(t, filepath.Join(layout.Metadata, "checkm2.parquet"), []checkFixtureRow{
		{SampleAccession: "SAMD00000692", Completeness: 99.5, Contamination: 0.4},
	})
	writeParquet(t, filepath.Join(layout.Metadata, "assembly_stats.parquet"), []assemblyStatsRow{
		{SampleAccession: "SAMD00000692", TotalLength: 5000000, N50: 120000},
	})
	row, err := (LocalStore{Layout: layout}).InfoRow(context.Background(), "SAMD00000692", false)
	if err != nil {
		t.Fatalf("InfoRow: %v", err)
	}
	if row["assembly_accession"] != "GCA_000692" {
		t.Fatalf("unexpected assembly info row: %#v", row)
	}
	if row["checkm2_completeness"] != 99.5 {
		t.Fatalf("expected checkm2 enrichment, got %#v", row)
	}
	if row["total_length"] != int64(5000000) {
		t.Fatalf("expected assembly stats enrichment, got %#v", row)
	}
	if _, err := os.Stat(layout.LookupDB); err != nil {
		t.Fatalf("expected lookup DB to be created, got %v", err)
	}
}

func TestLocalStoreRecordByIDUsesAssemblyMetadataFirst(t *testing.T) {
	layout := cache.NewLayout(t.TempDir())
	if err := layout.Ensure(); err != nil {
		t.Fatalf("Ensure: %v", err)
	}
	writeParquet(t, filepath.Join(layout.Metadata, "assembly.parquet"), []assemblyInfoRow{
		{
			SampleAccession: "SAMD00000692",
			RunAccession:    "DRR000692",
			ScientificName:  "Escherichia coli",
			HQFilter:        "pass",
		},
	})
	writeParquet(t, filepath.Join(layout.Metadata, "checkm2.parquet"), []checkFixtureRow{
		{SampleAccession: "SAMD00000692", Completeness: 99.5, Contamination: 0.4},
	})

	record, err := (LocalStore{Layout: layout}).RecordByID(context.Background(), "SAMD00000692")
	if err != nil {
		t.Fatalf("RecordByID: %v", err)
	}
	if record.SampleID != "SAMD00000692" {
		t.Fatalf("unexpected sample ID: %#v", record)
	}
	if record.GenomeID != "DRR000692" {
		t.Fatalf("unexpected genome ID: %#v", record)
	}
	if record.Species != "Escherichia coli" || record.Genus != "Escherichia" {
		t.Fatalf("unexpected species fields: %#v", record)
	}
	if !record.HQ {
		t.Fatalf("expected HQ record: %#v", record)
	}
	if record.CheckM2Completeness != 99.5 {
		t.Fatalf("expected CheckM2 enrichment: %#v", record)
	}
	if _, err := os.Stat(layout.LookupDB); err != nil {
		t.Fatalf("expected lookup DB to be created, got %v", err)
	}
}

func TestLocalStoreQueryRecordsUsesSQLiteQueryCache(t *testing.T) {
	layout := cache.NewLayout(t.TempDir())
	if err := layout.Ensure(); err != nil {
		t.Fatalf("Ensure: %v", err)
	}
	writeParquet(t, filepath.Join(layout.Metadata, "assembly.parquet"), []assemblyInfoRow{
		{SampleAccession: "S1", RunAccession: "R1", ScientificName: "Escherichia coli", HQFilter: "pass"},
		{SampleAccession: "S2", RunAccession: "R2", ScientificName: "Escherichia coli"},
		{SampleAccession: "S3", RunAccession: "R3", ScientificName: "Salmonella enterica"},
	})
	writeParquet(t, filepath.Join(layout.Metadata, "checkm2.parquet"), []checkFixtureRow{
		{SampleAccession: "S1", Completeness: 99, Contamination: 1},
		{SampleAccession: "S2", Completeness: 85, Contamination: 6},
		{SampleAccession: "S3", Completeness: 97, Contamination: 0.2},
	})

	records, err := (LocalStore{Layout: layout}).QueryRecords(context.Background(), model.Query{
		Species: "Escherichia coli",
		HQOnly:  true,
	})
	if err != nil {
		t.Fatalf("QueryRecords: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %#v", records)
	}
	if records[0].SampleID != "S1" || !records[0].HQ {
		t.Fatalf("unexpected records: %#v", records)
	}
	if _, err := os.Stat(layout.LookupDB); err != nil {
		t.Fatalf("expected lookup DB to be created, got %v", err)
	}
}

func TestEnsureLookupIndexLogsWhenBuildingFirstCache(t *testing.T) {
	layout := cache.NewLayout(t.TempDir())
	if err := layout.Ensure(); err != nil {
		t.Fatalf("Ensure: %v", err)
	}
	writeParquet(t, filepath.Join(layout.Metadata, "assembly.parquet"), []assemblyInfoRow{
		{SampleAccession: "S1", RunAccession: "R1", ScientificName: "Escherichia coli"},
	})

	var messages []string
	err := ensureLookupIndex(layout, func(format string, args ...any) {
		messages = append(messages, fmt.Sprintf(format, args...))
	})
	if err != nil {
		t.Fatalf("ensureLookupIndex: %v", err)
	}
	if len(messages) == 0 {
		t.Fatalf("expected a build log message")
	}
	if !strings.Contains(messages[0], "building local SQLite query cache") {
		t.Fatalf("unexpected log message: %q", messages[0])
	}
}

func writeParquet[T any](t *testing.T, path string, rows []T) {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create %s: %v", path, err)
	}
	writer := parquet.NewGenericWriter[T](f)
	if _, err := writer.Write(rows); err != nil {
		t.Fatalf("write rows to %s: %v", path, err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close writer for %s: %v", path, err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close file %s: %v", path, err)
	}
}

func writeGzipText(t *testing.T, path, text string) {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create %s: %v", path, err)
	}
	zw := gzip.NewWriter(f)
	if _, err := zw.Write([]byte(text)); err != nil {
		t.Fatalf("write gz text: %v", err)
	}
	if err := zw.Close(); err != nil {
		t.Fatalf("close gzip writer: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close %s: %v", path, err)
	}
}

func stringsJoin(lines ...string) string {
	return strings.Join(lines, "\n") + "\n"
}
