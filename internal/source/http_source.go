package source

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

type HTTPCatalog struct {
	URL    string
	Client *http.Client
}

func (h HTTPCatalog) LoadCatalog(ctx context.Context) (Catalog, error) {
	if h.URL == "" {
		return Catalog{}, fmt.Errorf("no source catalog configured; set --catalog-url or ATB_CATALOG_URL")
	}
	client := h.Client
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, h.URL, nil)
	if err != nil {
		return Catalog{}, fmt.Errorf("build catalog request: %w", err)
	}
	resp, err := client.Do(req)
	if err != nil {
		return Catalog{}, fmt.Errorf("load source catalog: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return Catalog{}, fmt.Errorf("load source catalog: unexpected HTTP status %s", resp.Status)
	}
	var cat Catalog
	if err := json.NewDecoder(resp.Body).Decode(&cat); err != nil {
		return Catalog{}, fmt.Errorf("decode source catalog: %w", err)
	}
	return cat, nil
}
