package download

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/martin/atb-cli-codex/internal/model"
	"github.com/ulikunitz/xz"
)

func TestPlanDownloadsAutoAndTarballGrouping(t *testing.T) {
	manifest := []model.AssemblyEntry{
		{SampleID: "S1", AWSURL: "https://example.org/S1.fa.gz", TarballName: "a.tar.gz", TarballURL: "https://example.org/a.tar.gz", FileInTarball: "S1.fa.gz"},
		{SampleID: "S2", AWSURL: "https://example.org/S2.fa.gz", TarballName: "a.tar.gz", TarballURL: "https://example.org/a.tar.gz", FileInTarball: "S2.fa.gz"},
		{SampleID: "S3", AWSURL: "https://example.org/S3.fa.gz", TarballName: "b.tar.gz", TarballURL: "https://example.org/b.tar.gz", FileInTarball: "S3.fa.gz"},
		{SampleID: "S4", AWSURL: "https://example.org/S4.fa.gz", TarballName: "b.tar.gz", TarballURL: "https://example.org/b.tar.gz", FileInTarball: "S4.fa.gz"},
		{SampleID: "S5", AWSURL: "https://example.org/S5.fa.gz", TarballName: "c.tar.gz", TarballURL: "https://example.org/c.tar.gz", FileInTarball: "S5.fa.gz"},
		{SampleID: "S6", AWSURL: "https://example.org/S6.fa.gz", TarballName: "c.tar.gz", TarballURL: "https://example.org/c.tar.gz", FileInTarball: "S6.fa.gz"},
	}
	plan, err := PlanDownloads([]string{"S1"}, manifest, "/tmp/genomes", StrategyAuto)
	if err != nil {
		t.Fatalf("PlanDownloads returned error: %v", err)
	}
	if plan.Strategy != StrategyAWS {
		t.Fatalf("expected aws strategy, got %s", plan.Strategy)
	}

	plan, err = PlanDownloads([]string{"S1", "S2", "S3", "S4", "S5", "S6"}, manifest, "/tmp/genomes", StrategyAuto)
	if err != nil {
		t.Fatalf("PlanDownloads returned error: %v", err)
	}
	if plan.Strategy != StrategyOSF || len(plan.Tarballs) != 3 {
		t.Fatalf("unexpected tarball plan: %#v", plan)
	}
}

func TestExecutorDownloadsAndExtracts(t *testing.T) {
	tmp := t.TempDir()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/S1.fa.gz":
			w.Write([]byte("aws-bytes"))
		case "/bundle.tar.gz":
			w.Write(buildTarGz(t, map[string]string{"bundle/S2.fa.gz": "tar-bytes"}))
		case "/bundle.tar.xz":
			w.Write(buildTarXz(t, map[string]string{"bundle/S3.fa": "xz-bytes"}))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	exec := Executor{Client: server.Client()}
	awsPlan := Plan{Strategy: StrategyAWS, AWS: []AWSItem{{SampleID: "S1", URL: server.URL + "/S1.fa.gz", DestPath: filepath.Join(tmp, "S1.fa.gz")}}}
	if err := exec.Execute(context.Background(), awsPlan, tmp); err != nil {
		t.Fatalf("Execute aws plan: %v", err)
	}
	data, _ := os.ReadFile(filepath.Join(tmp, "S1.fa.gz"))
	if string(data) != "aws-bytes" {
		t.Fatalf("unexpected aws download: %q", data)
	}

	osfPlan := Plan{Strategy: StrategyOSF, Tarballs: []TarballItem{{TarballName: "bundle.tar.gz", URL: server.URL + "/bundle.tar.gz", Files: []string{"bundle/S2.fa.gz"}}}}
	if err := exec.Execute(context.Background(), osfPlan, tmp); err != nil {
		t.Fatalf("Execute osf plan: %v", err)
	}
	data, _ = os.ReadFile(filepath.Join(tmp, "S2.fa.gz"))
	if string(data) != "tar-bytes" {
		t.Fatalf("unexpected extracted download: %q", data)
	}

	xzPlan := Plan{Strategy: StrategyOSF, Tarballs: []TarballItem{{TarballName: "bundle.tar.xz", URL: server.URL + "/bundle.tar.xz", Files: []string{"bundle/S3.fa"}}}}
	if err := exec.Execute(context.Background(), xzPlan, tmp); err != nil {
		t.Fatalf("Execute xz plan: %v", err)
	}
	data, _ = os.ReadFile(filepath.Join(tmp, "S3.fa"))
	if string(data) != "xz-bytes" {
		t.Fatalf("unexpected xz extracted download: %q", data)
	}
	if !strings.Contains(PlanCommand(awsPlan), "curl -L") {
		t.Fatalf("expected print command to contain curl")
	}
}

func buildTarGz(t *testing.T, files map[string]string) []byte {
	t.Helper()
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	for name, body := range files {
		if err := tw.WriteHeader(&tar.Header{Name: name, Mode: 0o644, Size: int64(len(body))}); err != nil {
			t.Fatalf("write header: %v", err)
		}
		if _, err := tw.Write([]byte(body)); err != nil {
			t.Fatalf("write body: %v", err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatalf("close tar writer: %v", err)
	}
	if err := gz.Close(); err != nil {
		t.Fatalf("close gzip writer: %v", err)
	}
	return buf.Bytes()
}

func buildTarXz(t *testing.T, files map[string]string) []byte {
	t.Helper()
	var tarBuf bytes.Buffer
	tw := tar.NewWriter(&tarBuf)
	for name, body := range files {
		if err := tw.WriteHeader(&tar.Header{Name: name, Mode: 0o644, Size: int64(len(body))}); err != nil {
			t.Fatalf("write header: %v", err)
		}
		if _, err := tw.Write([]byte(body)); err != nil {
			t.Fatalf("write body: %v", err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatalf("close tar writer: %v", err)
	}
	var out bytes.Buffer
	xzw, err := xz.NewWriter(&out)
	if err != nil {
		t.Fatalf("new xz writer: %v", err)
	}
	if _, err := xzw.Write(tarBuf.Bytes()); err != nil {
		t.Fatalf("write xz body: %v", err)
	}
	if err := xzw.Close(); err != nil {
		t.Fatalf("close xz writer: %v", err)
	}
	return out.Bytes()
}
