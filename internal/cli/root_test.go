package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/martin/atb-cli-codex/internal/cache"
	"github.com/martin/atb-cli-codex/internal/model"
)

func TestRootHelpIncludesExamples(t *testing.T) {
	cmd := NewRootCommand(context.Background())
	buf := &bytes.Buffer{}
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{"query", "--help"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("Execute help: %v", err)
	}
	out := buf.String()
	for _, want := range []string{
		`atb query --species "Escherichia coli" --sequence-type 131`,
		`atb query --species "Salmonella enterica" --limit 100 --sample-strategy even`,
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("help output missing %q\n%s", want, out)
		}
	}
}

func TestQueryAndDownloadCommands(t *testing.T) {
	cacheDir := t.TempDir()
	layout := cache.NewLayout(cacheDir)
	if err := layout.Ensure(); err != nil {
		t.Fatalf("Ensure layout: %v", err)
	}
	writeJSON(t, filepath.Join(layout.Metadata, "records.json"), []model.Record{
		{SampleID: "S1", GenomeID: "G1", Species: "Escherichia coli", Genus: "Escherichia", ASMFASTAOnOSF: 1, SequenceType: 131, HQ: true},
	})
	writeJSON(t, filepath.Join(layout.Manifests, "assemblies.json"), []model.AssemblyEntry{
		{SampleID: "S1", AWSURL: "https://example.org/S1.fa.gz", TarballName: "bundle.tar.gz", TarballURL: "https://example.org/bundle.tar.gz", FileInTarball: "bundle/S1.fa.gz"},
	})

	cmd := NewRootCommand(context.Background())
	buf := &bytes.Buffer{}
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{"--cache-dir", cacheDir, "query", "--species", "Escherichia coli", "--format", "json"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("query execute: %v", err)
	}
	if !strings.Contains(buf.String(), `"sample_id": "S1"`) {
		t.Fatalf("unexpected query output: %s", buf.String())
	}

	cmd = NewRootCommand(context.Background())
	buf = &bytes.Buffer{}
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{"--cache-dir", cacheDir, "download", "--sample", "S1", "--dry-run"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("download execute: %v", err)
	}
	if !strings.Contains(buf.String(), `"strategy": "aws"`) {
		t.Fatalf("unexpected dry-run output: %s", buf.String())
	}
}

func TestQueryDefaultsToTSVOutput(t *testing.T) {
	cacheDir := t.TempDir()
	layout := cache.NewLayout(cacheDir)
	if err := layout.Ensure(); err != nil {
		t.Fatalf("Ensure layout: %v", err)
	}
	writeJSON(t, filepath.Join(layout.Metadata, "records.json"), []model.Record{
		{SampleID: "S1", GenomeID: "G1", Species: "Escherichia coli", Genus: "Escherichia", ASMFASTAOnOSF: 1, HQ: true},
	})

	cmd := NewRootCommand(context.Background())
	buf := &bytes.Buffer{}
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{"--cache-dir", cacheDir, "query", "--species", "Escherichia coli"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("query execute: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "genome_id\t") {
		t.Fatalf("expected tab-delimited header, got %q", out)
	}
	if !strings.Contains(out, "G1\t") {
		t.Fatalf("expected tab-delimited row, got %q", out)
	}
}

func TestQueryActionableMissingCacheError(t *testing.T) {
	cmd := NewRootCommand(context.Background())
	buf := &bytes.Buffer{}
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{"--cache-dir", t.TempDir(), "query", "--species", "Escherichia coli"})
	err := cmd.Execute()
	if err == nil || !strings.Contains(err.Error(), "Run `atb fetch --metadata` first") {
		t.Fatalf("expected actionable cache error, got %v", err)
	}
}

func TestFetchReportsReusedCachedAssets(t *testing.T) {
	var serverURL string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/catalog.json":
			_, _ = w.Write([]byte(`{
				"metadata_version":"2025-05",
				"amr_version":"",
				"assets":[
					{"name":"ena_20240625.parquet","kind":"metadata","url":"` + serverURL + `/ena_20240625.parquet","version":"2025-05"},
					{"name":"file_list.all.latest.tsv.gz","kind":"manifest","url":"` + serverURL + `/file_list.all.latest.tsv.gz","version":"latest"}
				]
			}`))
		case "/ena_20240625.parquet", "/file_list.all.latest.tsv.gz":
			_, _ = w.Write([]byte("stub"))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	serverURL = server.URL

	cacheDir := t.TempDir()
	layout := cache.NewLayout(cacheDir)
	if err := layout.Ensure(); err != nil {
		t.Fatalf("Ensure layout: %v", err)
	}
	if err := os.WriteFile(filepath.Join(layout.Metadata, "ena_20240625.parquet"), []byte("cached"), 0o644); err != nil {
		t.Fatalf("write cached metadata: %v", err)
	}
	if err := os.WriteFile(filepath.Join(layout.Manifests, "file_list.all.latest.tsv.gz"), []byte("cached"), 0o644); err != nil {
		t.Fatalf("write cached manifest: %v", err)
	}

	cmd := NewRootCommand(context.Background())
	buf := &bytes.Buffer{}
	cmd.SetOut(buf)
	cmd.SetErr(&bytes.Buffer{})
	cmd.SetArgs([]string{
		"--cache-dir", cacheDir,
		"--catalog-url", server.URL + "/catalog.json",
		"fetch",
		"--metadata",
	})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("fetch execute: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "Reused cached: ena_20240625.parquet, file_list.all.latest.tsv.gz") {
		t.Fatalf("expected reused cached output, got %s", out)
	}
	if !strings.Contains(out, "Downloaded: none") {
		t.Fatalf("expected no downloads, got %s", out)
	}
}

func writeJSON(t *testing.T, path string, v any) {
	t.Helper()
	data, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal %s: %v", path, err)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}
