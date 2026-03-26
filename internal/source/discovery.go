package source

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"path"
	"sort"
	"strings"
	"time"
)

const (
	defaultOSFRootAPI         = "https://api.osf.io/v2/nodes/h7wzy/files/osfstorage/"
	defaultAssembliesRootAPI  = "https://api.osf.io/v2/nodes/zxfmy/files/osfstorage/"
	defaultGitHubAMRContents  = "https://api.github.com/repos/immem-hackathon-2025/atb-amr-shiny/git/trees/main?recursive=1"
	defaultMetadataFolderHint = "Latest_"
)

type DefaultCatalog struct {
	Client             *http.Client
	OSFRootAPI         string
	AssembliesRootAPI  string
	GitHubAMRContents  string
	MetadataFolderHint string
	IncludeMetadata    bool
	IncludeAMR         bool
	Genera             []string
	Logf               func(string, ...any)
}

func (d DefaultCatalog) LoadCatalog(ctx context.Context) (Catalog, error) {
	client := d.Client
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	}
	osfRoot := d.OSFRootAPI
	if osfRoot == "" {
		osfRoot = defaultOSFRootAPI
	}
	githubContents := d.GitHubAMRContents
	if githubContents == "" {
		githubContents = defaultGitHubAMRContents
	}
	assembliesRoot := d.AssembliesRootAPI
	if assembliesRoot == "" {
		assembliesRoot = defaultAssembliesRootAPI
	}
	folderHint := d.MetadataFolderHint
	if folderHint == "" {
		folderHint = defaultMetadataFolderHint
	}

	includeMetadata := d.IncludeMetadata
	includeAMR := d.IncludeAMR
	if !includeMetadata && !includeAMR {
		includeMetadata = true
		includeAMR = true
	}

	cat := Catalog{}
	if includeMetadata {
		if d.Logf != nil {
			d.Logf("discovering metadata parquet assets from OSF")
		}
		metadataAssets, metadataVersion, err := discoverMetadataAssets(ctx, client, osfRoot, folderHint)
		if err != nil {
			return Catalog{}, err
		}
		manifestAssets, err := discoverAssemblyManifestAssets(ctx, client, assembliesRoot)
		if err != nil {
			return Catalog{}, err
		}
		cat.MetadataVersion = metadataVersion
		cat.Assets = append(cat.Assets, metadataAssets...)
		cat.Assets = append(cat.Assets, manifestAssets...)
	}
	if includeAMR {
		if d.Logf != nil {
			d.Logf("discovering AMR parquet assets from GitHub")
		}
		amrAssets, amrVersion, err := discoverAMRAssets(ctx, client, githubContents, d.Genera)
		if err != nil {
			return Catalog{}, err
		}
		cat.AMRVersion = amrVersion
		cat.Assets = append(cat.Assets, amrAssets...)
	}
	return cat, nil
}

func discoverAssemblyManifestAssets(ctx context.Context, client *http.Client, rootURL string) ([]Asset, error) {
	root, err := fetchOSFList(ctx, client, rootURL)
	if err != nil {
		return nil, fmt.Errorf("discover assemblies root: %w", err)
	}
	fileLists, err := osfChildFolder(root, "File_Lists")
	if err != nil {
		return nil, err
	}
	list, err := fetchOSFList(ctx, client, fileLists.Relationships.Files.Links.Related.Href)
	if err != nil {
		return nil, fmt.Errorf("discover assembly file lists: %w", err)
	}
	var assets []Asset
	for _, item := range list.Data {
		if item.Attributes.Kind != "file" {
			continue
		}
		if item.Attributes.Name != "file_list.all.latest.tsv.gz" {
			continue
		}
		assets = append(assets, Asset{
			Name:    item.Attributes.Name,
			Kind:    "manifest",
			URL:     item.Links.Download,
			Version: "latest",
		})
	}
	if len(assets) == 0 {
		return nil, fmt.Errorf("assembly file list manifest file_list.all.latest.tsv.gz not found")
	}
	return assets, nil
}

type osfList struct {
	Data []osfItem `json:"data"`
}

type osfItem struct {
	Attributes struct {
		Name string `json:"name"`
		Kind string `json:"kind"`
	} `json:"attributes"`
	Links struct {
		Download string `json:"download"`
	} `json:"links"`
	Relationships struct {
		Files struct {
			Links struct {
				Related struct {
					Href string `json:"href"`
				} `json:"related"`
			} `json:"links"`
		} `json:"files"`
	} `json:"relationships"`
}

type githubItem struct {
	Name        string `json:"name"`
	Path        string `json:"path"`
	Type        string `json:"type"`
	DownloadURL string `json:"download_url"`
	URL         string `json:"url"`
}

type githubTree struct {
	SHA  string         `json:"sha"`
	Tree []githubGitRef `json:"tree"`
}

type githubGitRef struct {
	Path string `json:"path"`
	Type string `json:"type"`
	URL  string `json:"url"`
}

func discoverMetadataAssets(ctx context.Context, client *http.Client, rootURL, folderHint string) ([]Asset, string, error) {
	root, err := fetchOSFList(ctx, client, rootURL)
	if err != nil {
		return nil, "", fmt.Errorf("discover metadata root: %w", err)
	}
	aggregated, err := osfChildFolder(root, "Aggregated")
	if err != nil {
		return nil, "", err
	}
	aggList, err := fetchOSFList(ctx, client, aggregated.Relationships.Files.Links.Related.Href)
	if err != nil {
		return nil, "", fmt.Errorf("discover aggregated metadata folders: %w", err)
	}
	latest, err := latestNamedFolder(aggList, folderHint)
	if err != nil {
		return nil, "", err
	}
	latestList, err := fetchOSFList(ctx, client, latest.Relationships.Files.Links.Related.Href)
	if err != nil {
		return nil, "", fmt.Errorf("discover latest metadata release contents: %w", err)
	}
	parquetFolder, err := parquetMetadataFolder(latestList)
	if err != nil {
		return nil, "", err
	}
	parquetList, err := fetchOSFList(ctx, client, parquetFolder.Relationships.Files.Links.Related.Href)
	if err != nil {
		return nil, "", fmt.Errorf("discover metadata parquet contents: %w", err)
	}

	var assets []Asset
	for _, item := range parquetList.Data {
		if item.Attributes.Kind != "file" || !strings.HasSuffix(item.Attributes.Name, ".parquet") {
			continue
		}
		assets = append(assets, Asset{
			Name:    item.Attributes.Name,
			Kind:    "metadata",
			URL:     item.Links.Download,
			Version: latest.Attributes.Name,
		})
	}
	sort.Slice(assets, func(i, j int) bool { return assets[i].Name < assets[j].Name })
	return assets, latest.Attributes.Name, nil
}

func discoverAMRAssets(ctx context.Context, client *http.Client, rootURL string, genera []string) ([]Asset, string, error) {
	var tree githubTree
	if err := getJSON(ctx, client, rootURL, &tree); err != nil {
		return nil, "", fmt.Errorf("discover AMR git tree: %w", err)
	}
	allowedGenera := map[string]bool{}
	for _, genus := range genera {
		allowedGenera[strings.ToLower(genus)] = true
	}
	var assets []Asset
	for _, item := range tree.Tree {
		if item.Type != "blob" {
			continue
		}
		if !strings.HasPrefix(item.Path, "data/amr_by_genus/Genus=") || !strings.HasSuffix(item.Path, ".parquet") {
			continue
		}
		parts := strings.Split(item.Path, "/")
		if len(parts) < 4 {
			continue
		}
		genusDir := parts[2]
		if !strings.HasPrefix(genusDir, "Genus=") {
			continue
		}
		genus := strings.TrimPrefix(genusDir, "Genus=")
		if len(allowedGenera) > 0 && !allowedGenera[strings.ToLower(genus)] {
			continue
		}
		fileName := parts[len(parts)-1]
		assets = append(assets, Asset{
			Name:    fmt.Sprintf("%s_%s", strings.ToLower(genus), fileName),
			Kind:    "amr",
			URL:     rawGitHubURL(item.Path),
			Version: tree.SHA,
			Genus:   genus,
		})
	}
	sort.Slice(assets, func(i, j int) bool {
		if assets[i].Genus != assets[j].Genus {
			return assets[i].Genus < assets[j].Genus
		}
		return assets[i].Name < assets[j].Name
	})
	return assets, tree.SHA, nil
}

func fetchOSFList(ctx context.Context, client *http.Client, rawURL string) (osfList, error) {
	var out osfList
	if err := getJSON(ctx, client, rawURL, &out); err != nil {
		return osfList{}, err
	}
	return out, nil
}

func osfChildFolder(list osfList, name string) (osfItem, error) {
	for _, item := range list.Data {
		if item.Attributes.Kind == "folder" && item.Attributes.Name == name {
			return item, nil
		}
	}
	return osfItem{}, fmt.Errorf("OSF folder %q not found", name)
}

func latestNamedFolder(list osfList, prefix string) (osfItem, error) {
	var folders []osfItem
	for _, item := range list.Data {
		if item.Attributes.Kind == "folder" && strings.HasPrefix(item.Attributes.Name, prefix) {
			folders = append(folders, item)
		}
	}
	if len(folders) == 0 {
		return osfItem{}, fmt.Errorf("no OSF folder found with prefix %q", prefix)
	}
	sort.Slice(folders, func(i, j int) bool { return folders[i].Attributes.Name > folders[j].Attributes.Name })
	return folders[0], nil
}

func parquetMetadataFolder(list osfList) (osfItem, error) {
	for _, item := range list.Data {
		if item.Attributes.Kind == "folder" && strings.HasSuffix(item.Attributes.Name, ".parquet") {
			return item, nil
		}
	}
	return osfItem{}, fmt.Errorf("metadata parquet folder not found in latest OSF release")
}

func getJSON(ctx context.Context, client *http.Client, rawURL string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	if isGitHubAPI(rawURL) {
		req.Header.Set("Accept", "application/vnd.github+json")
	}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("GET %s: %w", redactURL(rawURL), err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("GET %s: unexpected HTTP status %s", redactURL(rawURL), resp.Status)
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return fmt.Errorf("decode %s: %w", redactURL(rawURL), err)
	}
	return nil
}

func isGitHubAPI(rawURL string) bool {
	u, err := url.Parse(rawURL)
	return err == nil && u.Host == "api.github.com"
}

func redactURL(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return rawURL
	}
	return u.Scheme + "://" + u.Host + path.Clean(u.Path)
}

func rawGitHubURL(repoPath string) string {
	escapedPath := strings.ReplaceAll(repoPath, "=", "%3D")
	return "https://raw.githubusercontent.com/immem-hackathon-2025/atb-amr-shiny/main/" + escapedPath
}
