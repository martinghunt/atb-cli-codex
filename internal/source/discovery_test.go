package source

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestDefaultCatalogDiscoversCanonicalAssets(t *testing.T) {
	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	defer server.Close()

	writeJSON := func(path string, value any) {
		t.Helper()
		mux.HandleFunc(path, func(w http.ResponseWriter, r *http.Request) {
			_ = json.NewEncoder(w).Encode(value)
		})
	}

	writeJSON("/osf/root", osfList{Data: []osfItem{
		osfFolder("Aggregated", server.URL+"/osf/aggregated"),
	}})
	writeJSON("/osf/aggregated", osfList{Data: []osfItem{
		osfFolder("2024-08", server.URL+"/osf/old"),
		osfFolder("Latest_2025-05", server.URL+"/osf/latest"),
	}})
	writeJSON("/osf/latest", osfList{Data: []osfItem{
		osfFolder("atb.metadata.202505.parquet", server.URL+"/osf/parquet"),
	}})
	writeJSON("/osf/parquet", osfList{Data: []osfItem{
		osfFile("assembly_stats.parquet", server.URL+"/download/assembly_stats.parquet"),
		osfFile("checkm2.parquet", server.URL+"/download/checkm2.parquet"),
		osfFile("ena_202505_used.parquet", server.URL+"/download/ena_202505_used.parquet"),
	}})

	writeJSON("/github/amr", []githubItem{})
	writeJSON("/github/amr-tree", githubTree{
		SHA: "abc123",
		Tree: []githubGitRef{
			{Path: "data/amr_by_genus/Genus=Escherichia/data_0.parquet", Type: "blob"},
			{Path: "data/amr_by_genus/Genus=Salmonella/data_0.parquet", Type: "blob"},
		},
	})
	writeJSON("/assemblies/root", osfList{Data: []osfItem{
		osfFolder("File_Lists", server.URL+"/assemblies/file_lists"),
	}})
	writeJSON("/assemblies/file_lists", osfList{Data: []osfItem{
		osfFile("file_list.all.latest.tsv.gz", server.URL+"/download/file_list.all.latest.tsv.gz"),
	}})

	cat, err := DefaultCatalog{
		Client:             server.Client(),
		OSFRootAPI:         server.URL + "/osf/root",
		AssembliesRootAPI:  server.URL + "/assemblies/root",
		GitHubAMRContents:  server.URL + "/github/amr-tree",
		MetadataFolderHint: "Latest_",
	}.LoadCatalog(context.Background())
	if err != nil {
		t.Fatalf("LoadCatalog: %v", err)
	}
	if cat.MetadataVersion != "Latest_2025-05" {
		t.Fatalf("unexpected metadata version: %#v", cat)
	}
	if cat.AMRVersion != "abc123" {
		t.Fatalf("unexpected AMR version: %#v", cat)
	}
	if len(cat.Assets) != 6 {
		t.Fatalf("expected 6 assets, got %#v", cat.Assets)
	}
	if cat.Assets[3].Kind != "manifest" {
		t.Fatalf("expected manifest asset before AMR assets, got %#v", cat.Assets[3])
	}
	if got := cat.Assets[4].Genus; got != "Escherichia" {
		t.Fatalf("expected Escherichia genus asset, got %#v", cat.Assets[4])
	}
}

func TestIsGitHubAPI(t *testing.T) {
	if !isGitHubAPI("https://api.github.com/repos/example/repo/contents") {
		t.Fatal("expected GitHub API URL to be detected")
	}
	if isGitHubAPI("https://osf.io/download/abc") {
		t.Fatal("did not expect non-GitHub URL to be detected")
	}
}

func TestDefaultCatalogMetadataOnlySkipsAMRDiscovery(t *testing.T) {
	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	defer server.Close()

	writeJSON := func(path string, value any) {
		t.Helper()
		mux.HandleFunc(path, func(w http.ResponseWriter, r *http.Request) {
			_ = json.NewEncoder(w).Encode(value)
		})
	}

	writeJSON("/osf/root", osfList{Data: []osfItem{
		osfFolder("Aggregated", server.URL+"/osf/aggregated"),
	}})
	writeJSON("/osf/aggregated", osfList{Data: []osfItem{
		osfFolder("Latest_2025-05", server.URL+"/osf/latest"),
	}})
	writeJSON("/osf/latest", osfList{Data: []osfItem{
		osfFolder("atb.metadata.202505.parquet", server.URL+"/osf/parquet"),
	}})
	writeJSON("/osf/parquet", osfList{Data: []osfItem{
		osfFile("assembly_stats.parquet", server.URL+"/download/assembly_stats.parquet"),
	}})
	writeJSON("/assemblies/root", osfList{Data: []osfItem{
		osfFolder("File_Lists", server.URL+"/assemblies/file_lists"),
	}})
	writeJSON("/assemblies/file_lists", osfList{Data: []osfItem{
		osfFile("file_list.all.latest.tsv.gz", server.URL+"/download/file_list.all.latest.tsv.gz"),
	}})
	mux.HandleFunc("/github/amr-tree", func(w http.ResponseWriter, r *http.Request) {
		t.Fatalf("metadata-only discovery should not call the AMR source")
	})

	cat, err := DefaultCatalog{
		Client:             server.Client(),
		OSFRootAPI:         server.URL + "/osf/root",
		AssembliesRootAPI:  server.URL + "/assemblies/root",
		GitHubAMRContents:  server.URL + "/github/amr-tree",
		MetadataFolderHint: "Latest_",
		IncludeMetadata:    true,
		IncludeAMR:         false,
	}.LoadCatalog(context.Background())
	if err != nil {
		t.Fatalf("LoadCatalog: %v", err)
	}
	if len(cat.Assets) != 2 {
		t.Fatalf("expected only metadata and manifest assets, got %#v", cat.Assets)
	}
	if cat.AMRVersion != "" {
		t.Fatalf("expected empty AMR version for metadata-only discovery, got %#v", cat)
	}
}

func TestDefaultCatalogAMRGenusFilterUsesSingleTree(t *testing.T) {
	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	defer server.Close()

	writeJSON := func(path string, value any) {
		t.Helper()
		mux.HandleFunc(path, func(w http.ResponseWriter, r *http.Request) {
			_ = json.NewEncoder(w).Encode(value)
		})
	}

	writeJSON("/github/amr-tree", githubTree{
		SHA: "tree-sha",
		Tree: []githubGitRef{
			{Path: "data/amr_by_genus/Genus=Escherichia/data_0.parquet", Type: "blob"},
			{Path: "data/amr_by_genus/Genus=Salmonella/data_0.parquet", Type: "blob"},
			{Path: "data/amr_by_genus/Genus=Escherichia/notes.txt", Type: "blob"},
		},
	})

	cat, err := DefaultCatalog{
		Client:            server.Client(),
		GitHubAMRContents: server.URL + "/github/amr-tree",
		IncludeMetadata:   false,
		IncludeAMR:        true,
		Genera:            []string{"Escherichia"},
	}.LoadCatalog(context.Background())
	if err != nil {
		t.Fatalf("LoadCatalog: %v", err)
	}
	if len(cat.Assets) != 1 {
		t.Fatalf("expected one filtered AMR asset, got %#v", cat.Assets)
	}
	if cat.Assets[0].Genus != "Escherichia" {
		t.Fatalf("unexpected filtered AMR asset: %#v", cat.Assets[0])
	}
	if cat.AMRVersion != "tree-sha" {
		t.Fatalf("unexpected tree SHA version: %#v", cat)
	}
}

func osfFolder(name, href string) osfItem {
	var item osfItem
	item.Attributes.Name = name
	item.Attributes.Kind = "folder"
	item.Relationships.Files.Links.Related.Href = href
	return item
}

func osfFile(name, download string) osfItem {
	var item osfItem
	item.Attributes.Name = name
	item.Attributes.Kind = "file"
	item.Links.Download = download
	return item
}
