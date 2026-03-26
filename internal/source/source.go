package source

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/martin/atb-cli-codex/internal/cache"
	"github.com/martin/atb-cli-codex/internal/model"
)

type Asset struct {
	Name    string `json:"name"`
	Kind    string `json:"kind"`
	URL     string `json:"url"`
	Version string `json:"version"`
	Genus   string `json:"genus,omitempty"`
}

type Catalog struct {
	MetadataVersion string  `json:"metadata_version"`
	AMRVersion      string  `json:"amr_version"`
	Assets          []Asset `json:"assets"`
}

type CatalogSource interface {
	LoadCatalog(ctx context.Context) (Catalog, error)
}

type Downloader interface {
	Download(ctx context.Context, url string, dest string) error
}

type HTTPDownloader struct {
	Client *http.Client
	Logf   func(string, ...any)
}

func (d HTTPDownloader) Download(ctx context.Context, url string, dest string) error {
	client := d.Client
	if client == nil {
		client = &http.Client{}
	}
	if d.Logf != nil {
		d.Logf("downloading %s -> %s", url, dest)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("download %s: %w", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("download %s: unexpected HTTP status %s", url, resp.Status)
	}
	if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
		return fmt.Errorf("create destination directory: %w", err)
	}
	f, err := os.Create(dest)
	if err != nil {
		return fmt.Errorf("create destination file: %w", err)
	}
	defer f.Close()
	if _, err := io.Copy(f, resp.Body); err != nil {
		return fmt.Errorf("write %s: %w", dest, err)
	}
	if d.Logf != nil {
		d.Logf("finished %s", dest)
	}
	return nil
}

type SyncResult struct {
	Downloaded []string
	Skipped    []string
	State      model.State
}

type Syncer struct {
	Layout     cache.Layout
	Catalog    CatalogSource
	Downloader Downloader
	Now        func() time.Time
	Logf       func(string, ...any)
}

func (s Syncer) Fetch(ctx context.Context, metadata, amr bool, genera []string, force bool) (SyncResult, error) {
	if !metadata && !amr {
		return SyncResult{}, errors.New("choose at least one of --metadata or --amr")
	}
	if err := s.Layout.Ensure(); err != nil {
		return SyncResult{}, err
	}
	if s.Logf != nil {
		s.Logf("discovering sources metadata=%t amr=%t", metadata, amr)
	}
	cat, err := s.Catalog.LoadCatalog(ctx)
	if err != nil {
		return SyncResult{}, err
	}
	state, err := s.Layout.ReadState()
	if err != nil {
		return SyncResult{}, err
	}
	allowedGenus := map[string]bool{}
	for _, genus := range genera {
		allowedGenus[strings.ToLower(genus)] = true
	}
	var downloaded []string
	var skipped []string
	for _, asset := range cat.Assets {
		if asset.Kind == "metadata" && !metadata {
			continue
		}
		if asset.Kind == "manifest" && !metadata {
			continue
		}
		if asset.Kind == "amr" && !amr {
			continue
		}
		if asset.Kind == "amr" && len(allowedGenus) > 0 && !allowedGenus[strings.ToLower(asset.Genus)] {
			continue
		}
		dest, err := assetDestination(s.Layout, asset)
		if err != nil {
			return SyncResult{}, err
		}
		if !force {
			if _, err := os.Stat(dest); err == nil {
				if s.Logf != nil {
					s.Logf("using cached %s", dest)
				}
				skipped = append(skipped, asset.Name)
				continue
			}
		}
		if s.Logf != nil {
			s.Logf("fetching asset kind=%s name=%s", asset.Kind, asset.Name)
		}
		if err := s.Downloader.Download(ctx, asset.URL, dest); err != nil {
			return SyncResult{}, err
		}
		downloaded = append(downloaded, asset.Name)
	}
	if metadata {
		state.MetadataVersion = cat.MetadataVersion
	}
	if amr {
		state.AMRVersion = cat.AMRVersion
	}
	if state.Sources == nil {
		state.Sources = map[string]string{}
	}
	for _, asset := range cat.Assets {
		if asset.Kind == "metadata" || asset.Kind == "manifest" {
			state.Sources[asset.Name] = asset.URL
			continue
		}
		if asset.Kind == "amr" {
			state.Sources["amr:"+asset.Genus] = asset.URL
		}
	}
	now := s.Now
	if now == nil {
		now = time.Now
	}
	state.UpdatedAt = now().UTC().Format(time.RFC3339)
	if err := s.Layout.WriteState(state); err != nil {
		return SyncResult{}, err
	}
	return SyncResult{Downloaded: downloaded, Skipped: skipped, State: state}, nil
}

func assetDestination(layout cache.Layout, asset Asset) (string, error) {
	switch asset.Kind {
	case "metadata":
		return filepath.Join(layout.Metadata, asset.Name), nil
	case "amr":
		return filepath.Join(layout.AMR, asset.Name), nil
	case "manifest":
		return filepath.Join(layout.Manifests, asset.Name), nil
	default:
		return "", fmt.Errorf("unsupported asset kind %q", asset.Kind)
	}
}
