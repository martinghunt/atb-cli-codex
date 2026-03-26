package source

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/martin/atb-cli-codex/internal/cache"
)

func TestSyncerFetchAndReuseCache(t *testing.T) {
	cat := Catalog{
		MetadataVersion: "2026-03-01",
		AMRVersion:      "2026-03-02",
		Assets: []Asset{
			{Name: "records.json", Kind: "metadata", URL: "/records.json"},
			{Name: "assemblies.json", Kind: "manifest", URL: "/assemblies.json"},
			{Name: "escherichia.json", Kind: "amr", URL: "/escherichia.json", Genus: "Escherichia"},
		},
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/catalog.json":
			_ = json.NewEncoder(w).Encode(cat)
		case "/records.json", "/assemblies.json", "/escherichia.json":
			w.Write([]byte("{}"))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	for i := range cat.Assets {
		cat.Assets[i].URL = server.URL + cat.Assets[i].URL
	}

	layout := cache.NewLayout(t.TempDir())
	syncer := Syncer{
		Layout:     layout,
		Catalog:    HTTPCatalog{URL: server.URL + "/catalog.json", Client: server.Client()},
		Downloader: HTTPDownloader{Client: server.Client()},
		Now:        func() time.Time { return time.Date(2026, 3, 26, 0, 0, 0, 0, time.UTC) },
	}

	result, err := syncer.Fetch(context.Background(), true, true, nil, false)
	if err != nil {
		t.Fatalf("Fetch returned error: %v", err)
	}
	if len(result.Downloaded) != 3 {
		t.Fatalf("expected 3 downloads, got %#v", result)
	}
	if _, err := os.Stat(filepath.Join(layout.Metadata, "records.json")); err != nil {
		t.Fatalf("expected metadata file: %v", err)
	}

	result, err = syncer.Fetch(context.Background(), true, true, nil, false)
	if err != nil {
		t.Fatalf("Fetch returned error: %v", err)
	}
	if len(result.Skipped) != 3 {
		t.Fatalf("expected files to be reused, got %#v", result)
	}
}
