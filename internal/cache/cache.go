package cache

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/martin/atb-cli-codex/internal/model"
)

type Layout struct {
	Root      string
	Metadata  string
	AMR       string
	Manifests string
	Genomes   string
	Indexes   string
	StateFile string
	LookupDB  string
}

func DefaultRoot() (string, error) {
	root, err := os.UserCacheDir()
	if err != nil {
		return "", fmt.Errorf("resolve user cache dir: %w", err)
	}
	return filepath.Join(root, "atb"), nil
}

func NewLayout(root string) Layout {
	return Layout{
		Root:      root,
		Metadata:  filepath.Join(root, "metadata"),
		AMR:       filepath.Join(root, "amr"),
		Manifests: filepath.Join(root, "manifests"),
		Genomes:   filepath.Join(root, "genomes"),
		Indexes:   filepath.Join(root, "indexes"),
		StateFile: filepath.Join(root, "state.json"),
		LookupDB:  filepath.Join(root, "indexes", "lookup.sqlite"),
	}
}

func (l Layout) Ensure() error {
	for _, dir := range []string{l.Root, l.Metadata, l.AMR, l.Manifests, l.Genomes, l.Indexes} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("create cache directory %s: %w", dir, err)
		}
	}
	return nil
}

func (l Layout) ReadState() (model.State, error) {
	var state model.State
	data, err := os.ReadFile(l.StateFile)
	if err != nil {
		if os.IsNotExist(err) {
			return model.State{}, nil
		}
		return state, fmt.Errorf("read state file: %w", err)
	}
	if err := json.Unmarshal(data, &state); err != nil {
		return state, fmt.Errorf("decode state file: %w", err)
	}
	return state, nil
}

func (l Layout) WriteState(state model.State) error {
	if err := l.Ensure(); err != nil {
		return err
	}
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return fmt.Errorf("encode state file: %w", err)
	}
	if err := os.WriteFile(l.StateFile, data, 0o644); err != nil {
		return fmt.Errorf("write state file: %w", err)
	}
	return nil
}
