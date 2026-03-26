package download

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/martin/atb-cli-codex/internal/model"
	"github.com/ulikunitz/xz"
)

const AutoAWSLimit = 5

type Strategy string

const (
	StrategyAuto Strategy = "auto"
	StrategyAWS  Strategy = "aws"
	StrategyOSF  Strategy = "osf-tarball"
)

type Plan struct {
	Strategy Strategy      `json:"strategy"`
	Reason   string        `json:"reason"`
	AWS      []AWSItem     `json:"aws,omitempty"`
	Tarballs []TarballItem `json:"tarballs,omitempty"`
}

type AWSItem struct {
	SampleID string `json:"sample_id"`
	URL      string `json:"url"`
	DestPath string `json:"dest_path"`
}

type TarballItem struct {
	TarballName string   `json:"tarball_name"`
	URL         string   `json:"url"`
	Files       []string `json:"files"`
}

func PlanDownloads(samples []string, manifest []model.AssemblyEntry, genomesDir string, strategy Strategy) (Plan, error) {
	samples = dedupe(samples)
	if len(samples) == 0 {
		return Plan{}, fmt.Errorf("no sample IDs supplied; use --sample, --input, or stdin")
	}
	entries := map[string]model.AssemblyEntry{}
	for _, entry := range manifest {
		entries[entry.SampleID] = entry
	}
	switch strategy {
	case StrategyAuto:
		if len(samples) <= AutoAWSLimit {
			p, err := PlanDownloads(samples, manifest, genomesDir, StrategyAWS)
			if err != nil {
				return Plan{}, err
			}
			p.Reason = fmt.Sprintf("auto chose aws because %d requested genomes is at or below the threshold of %d", len(samples), AutoAWSLimit)
			return p, nil
		}
		p, err := PlanDownloads(samples, manifest, genomesDir, StrategyOSF)
		if err != nil {
			return Plan{}, err
		}
		p.Reason = fmt.Sprintf("auto chose osf-tarball because %d requested genomes is above the threshold of %d", len(samples), AutoAWSLimit)
		return p, nil
	case StrategyAWS:
		items := make([]AWSItem, 0, len(samples))
		for _, sample := range samples {
			entry, ok := entries[sample]
			if !ok {
				return Plan{}, fmt.Errorf("sample %q is not present in the assembly manifest", sample)
			}
			url := entry.AWSURL
			if url == "" {
				url = fmt.Sprintf("https://allthebacteria-assemblies.s3.amazonaws.com/%s.fa.gz", sample)
			}
			items = append(items, AWSItem{
				SampleID: sample,
				URL:      url,
				DestPath: filepath.Join(genomesDir, sample+".fa.gz"),
			})
		}
		sort.Slice(items, func(i, j int) bool { return items[i].SampleID < items[j].SampleID })
		return Plan{Strategy: StrategyAWS, AWS: items, Reason: "explicit aws strategy requested"}, nil
	case StrategyOSF:
		grouped := map[string]*TarballItem{}
		for _, sample := range samples {
			entry, ok := entries[sample]
			if !ok {
				return Plan{}, fmt.Errorf("sample %q is not present in the assembly manifest", sample)
			}
			item, ok := grouped[entry.TarballName]
			if !ok {
				item = &TarballItem{TarballName: entry.TarballName, URL: entry.TarballURL}
				grouped[entry.TarballName] = item
			}
			item.Files = append(item.Files, entry.FileInTarball)
		}
		names := make([]string, 0, len(grouped))
		for name := range grouped {
			names = append(names, name)
		}
		sort.Strings(names)
		var tarballs []TarballItem
		for _, name := range names {
			item := grouped[name]
			sort.Strings(item.Files)
			tarballs = append(tarballs, *item)
		}
		return Plan{Strategy: StrategyOSF, Tarballs: tarballs, Reason: "explicit osf-tarball strategy requested"}, nil
	default:
		return Plan{}, fmt.Errorf("unsupported --strategy %q; valid values are auto, aws, osf-tarball", strategy)
	}
}

func PlanCommand(plan Plan) string {
	switch plan.Strategy {
	case StrategyAWS:
		lines := make([]string, 0, len(plan.AWS))
		for _, item := range plan.AWS {
			lines = append(lines, fmt.Sprintf("curl -L %q -o %q", item.URL, item.DestPath))
		}
		return strings.Join(lines, "\n")
	case StrategyOSF:
		lines := make([]string, 0, len(plan.Tarballs))
		for _, item := range plan.Tarballs {
			lines = append(lines, fmt.Sprintf("# %s: %s", item.TarballName, strings.Join(item.Files, ", ")))
			lines = append(lines, fmt.Sprintf("curl -L %q -o %q", item.URL, item.TarballName))
		}
		return strings.Join(lines, "\n")
	default:
		return ""
	}
}

type Executor struct {
	Client *http.Client
}

func (e Executor) Execute(ctx context.Context, plan Plan, genomesDir string) error {
	client := e.Client
	if client == nil {
		client = &http.Client{Timeout: 60 * time.Second}
	}
	if err := os.MkdirAll(genomesDir, 0o755); err != nil {
		return fmt.Errorf("create genomes directory: %w", err)
	}
	switch plan.Strategy {
	case StrategyAWS:
		for _, item := range plan.AWS {
			if err := downloadToFile(ctx, client, item.URL, item.DestPath); err != nil {
				return err
			}
		}
	case StrategyOSF:
		for _, item := range plan.Tarballs {
			tmpPath := filepath.Join(os.TempDir(), item.TarballName)
			if err := downloadToFile(ctx, client, item.URL, tmpPath); err != nil {
				return err
			}
			if err := extractFiles(tmpPath, genomesDir, item.Files); err != nil {
				return err
			}
			_ = os.Remove(tmpPath)
		}
	default:
		return fmt.Errorf("unsupported strategy %q", plan.Strategy)
	}
	return nil
}

func downloadToFile(ctx context.Context, client *http.Client, url, dest string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("download %s: %w", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("download %s: unexpected HTTP status %s", url, resp.Status)
	}
	out, err := os.Create(dest)
	if err != nil {
		return err
	}
	defer out.Close()
	_, err = io.Copy(out, resp.Body)
	return err
}

func extractFiles(tarballPath, genomesDir string, wanted []string) error {
	set := map[string]bool{}
	for _, path := range wanted {
		set[path] = true
	}
	f, err := os.Open(tarballPath)
	if err != nil {
		return err
	}
	defer f.Close()
	tr, closeFn, err := openTarReader(f, tarballPath)
	if err != nil {
		return err
	}
	defer closeFn()
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if !set[hdr.Name] {
			continue
		}
		dest := filepath.Join(genomesDir, filepath.Base(hdr.Name))
		out, err := os.Create(dest)
		if err != nil {
			return err
		}
		if _, err := io.Copy(out, tr); err != nil {
			out.Close()
			return err
		}
		if err := out.Close(); err != nil {
			return err
		}
	}
}

func openTarReader(f *os.File, tarballPath string) (*tar.Reader, func() error, error) {
	switch {
	case strings.HasSuffix(tarballPath, ".tar.gz"), strings.HasSuffix(tarballPath, ".tgz"):
		gr, err := gzip.NewReader(f)
		if err != nil {
			return nil, nil, err
		}
		return tar.NewReader(gr), gr.Close, nil
	case strings.HasSuffix(tarballPath, ".tar.xz"), strings.HasSuffix(tarballPath, ".txz"), strings.HasSuffix(tarballPath, ".xz"):
		xzr, err := xz.NewReader(f)
		if err != nil {
			return nil, nil, err
		}
		return tar.NewReader(xzr), func() error { return nil }, nil
	default:
		return tar.NewReader(f), func() error { return nil }, nil
	}
}

func dedupe(items []string) []string {
	set := map[string]bool{}
	out := make([]string, 0, len(items))
	for _, item := range items {
		item = strings.TrimSpace(item)
		if item == "" || set[item] {
			continue
		}
		set[item] = true
		out = append(out, item)
	}
	sort.Strings(out)
	return out
}
