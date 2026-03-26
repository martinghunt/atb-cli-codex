# atb-cli-codex

`atb-cli-codex` is a Go CLI for querying and downloading AllTheBacteria data with a strong focus on:
- user-friendly commands
- reproducible queries
- cached local data
- tab-delimited default output
- testable business logic

## Install

### Download a release binary

GitHub releases publish binaries for:
- macOS `amd64`
- macOS `arm64`
- Linux `amd64`
- Linux `arm64`
- Windows `amd64`
- Windows `arm64`

Download the binary for your platform from the Releases page, make it executable, and place it on your `PATH`.

Example on macOS or Linux:

```bash
curl -L -o atb https://github.com/martinghunt/atb-cli-codex/releases/latest/download/atb-darwin-arm64
chmod +x atb
mv atb /usr/local/bin/
```

Adjust the asset name for your platform:
- `atb-darwin-amd64`
- `atb-darwin-arm64`
- `atb-linux-amd64`
- `atb-linux-arm64`
- `atb-windows-amd64.exe`
- `atb-windows-arm64.exe`

### Build from source

Requirements:
- Go 1.26+

Build for the current platform:

```bash
bash ./build.sh
./dist/$(go env GOOS)-$(go env GOARCH)/atb --help
```

Build all supported binaries:

```bash
bash ./build.sh --all
```

## Quick start

Fetch metadata first:

```bash
atb fetch --metadata
```

This downloads the metadata parquet files into the local cache and now also builds the local SQLite query cache. The first metadata fetch can take a while because it prepares that cache for later fast lookups.

Then run queries:

```bash
atb query --species "Escherichia coli"
atb stats --species "Escherichia coli" --hq-only
atb info --sample SAMD00000692
```

## Cache layout

By default, data is stored under the OS user cache directory, for example:

```text
~/Library/Caches/atb
```

Override it with:
- `--cache-dir`
- `ATB_CACHE_DIR`

Important cache contents:
- `metadata/` for cached parquet files
- `amr/` for cached AMR partitions
- `manifests/` for assembly manifests
- `indexes/lookup.sqlite` for the local SQLite query cache
- `genomes/` for downloaded genome FASTA files

## Commands

### Fetch and update

Fetch metadata, AMR, or both:

```bash
atb fetch --metadata
atb fetch --amr --genus escherichia
atb fetch --metadata --amr
```

Refresh cached data:

```bash
atb update --metadata
atb update --amr
atb update --metadata --amr
```

### Query

Default output is TSV.

Examples:

```bash
atb query --species "Escherichia coli"
atb query --species "Escherichia coli" --hq-only
atb query --species "Escherichia coli" --sequence-type 131
atb query --species "Salmonella enterica" --limit 100 --sample-strategy even
atb query --sample-id SAMD00000692
atb query --genome-id DRR000692
```

Useful filters:
- `--species`
- `--sample-id`
- `--genome-id`
- `--sequence-type`
- `--hq-only`
- `--checkm2-min`
- `--checkm2-max-contamination`
- `--asm-fasta-on-osf 1|0|any`
- `--limit`
- `--sample-strategy all|even`

Important default:
- query-style workflows effectively default to `--asm-fasta-on-osf 1`
- use `--asm-fasta-on-osf any` to include samples without `asm_fasta_on_osf=1`

Examples:

```bash
atb query --species "Escherichia coli" --asm-fasta-on-osf any
atb query --species "Escherichia coli" --checkm2-min 95 --checkm2-max-contamination 5
atb query --species "Escherichia coli" --format json
atb query --species "Escherichia coli" --format csv > ecoli.csv
```

### AMR and MLST shortcuts

These are thin wrappers over shared query logic:

```bash
atb amr --species "Klebsiella pneumoniae" --hq-only
atb mlst --species "Escherichia coli"
```

### Info

Show cached metadata for one sample:

```bash
atb info --sample SAMD00000692
```

By default this uses assembly/checkm2/assembly_stats-backed cached metadata and avoids ENA scans. To include ENA-derived fields too:

```bash
atb info --sample SAMD00000692 --include-ena
```

### Stats

Summarize the whole dataset or a filtered subset:

```bash
atb stats
atb stats --species "Escherichia coli"
atb stats --species "Escherichia coli" --hq-only
```

Current stats output includes:
- total genomes
- HQ vs non-HQ counts
- CheckM2 threshold counts
- counts per species
- counts per genus
- top species
- field coverage for key cached metadata fields

### Download

Download FASTA files from sample IDs:

```bash
atb download --sample SAMD00000344
atb download --input ecoli.csv --dry-run
atb download --input ecoli.csv --print-command
atb download --input salmonella.csv --strategy auto
```

Input sources:
- `--sample`
- `--input <file>`
- `stdin`

Strategies:
- `auto`
- `aws`
- `osf-tarball`

## Reproducible queries

Emit or save the effective query:

```bash
atb query --species "Escherichia coli" --hq-only --emit-query-toml
atb query --species "Escherichia coli" --hq-only --save-query ecoli.toml
atb query --query-file ecoli.toml
```

The saved query representation is useful for rerunning the same workflow later against a known local cache.

## Output formats

Supported formats:
- `tsv`
- `csv`
- `json`
- `table`

Examples:

```bash
atb query --species "Escherichia coli" --format tsv
atb query --species "Escherichia coli" --format csv
atb query --species "Escherichia coli" --format json
```

Notes:
- default output is `tsv`
- `table` is also tab-delimited in this CLI rather than padded pretty-print output

## Development

Run tests:

```bash
go test ./...
```

GitHub Actions:
- CI runs on pull requests and pushes to `main`
- release workflow runs on tags matching `vX.Y.Z`
- tagged releases attach all built binaries as GitHub release assets

## Benchmarks

This section compares `atb-cli-codex` against the local checkout in `/Users/martin/atb-cli-claude`.

Tested on March 26, 2026 on the current macOS host using:
- `atb-cli-codex` commit `863e665`
- `atb-cli-claude` commit `577f45d`
- local ATB parquet cache at `/Users/martin/Library/Caches/atb/metadata`
- `/usr/bin/time -l` for wall time and maximum resident set size

The comparison only uses commands both CLIs support against the same local parquet cache.

### Method

Build commands:

```bash
go build -o /tmp/atb-codex ./cmd/atb
cd /Users/martin/atb-cli-claude && go build -o /tmp/atb-claude ./cmd/atb
```

`atb-cli-codex` query benchmarks used a temporary cache root with the real metadata directory mounted underneath it:

```bash
rm -rf /tmp/atb-bench-codex
mkdir -p /tmp/atb-bench-codex
ln -s /Users/martin/Library/Caches/atb/metadata /tmp/atb-bench-codex/metadata
mkdir -p /tmp/atb-bench-codex/manifests /tmp/atb-bench-codex/amr /tmp/atb-bench-codex/genomes
```

Measured commands:

```bash
/usr/bin/time -l /tmp/atb-codex --cache-dir /tmp/atb-bench-codex query \
  --species "Escherichia coli" --hq-only --limit 100 --format tsv >/dev/null

/usr/bin/time -l /tmp/atb-claude --data-dir /Users/martin/Library/Caches/atb/metadata query \
  --species "Escherichia coli" --hq-only --has-assembly --limit 100 --format tsv >/dev/null

/usr/bin/time -l /tmp/atb-codex --cache-dir /tmp/atb-bench-codex info \
  --sample SAMD00000692 --format tsv >/dev/null

/usr/bin/time -l /tmp/atb-claude --data-dir /Users/martin/Library/Caches/atb/metadata info \
  SAMD00000692 >/dev/null
```

### Results

#### Species query

Query shape:
- species = `Escherichia coli`
- HQ only
- limit = `100`
- both tools restricted to samples with assemblies available

| Tool | Mode | Wall time | Max RSS |
|---|---|---:|---:|
| `atb-cli-codex` | cold, first SQLite cache build | 102.59 s | 3228.7 MiB |
| `atb-cli-codex` | warm, query cache already built | 6.79 s | 1125.2 MiB |
| `atb-cli-claude` | warm | 2.96 s | 2016.0 MiB |

Interpretation:
- `atb-cli-codex` pays a heavy first-run cost to build `lookup.sqlite`.
- After the cache exists, it uses substantially less memory than `atb-cli-claude` for this query.
- `atb-cli-claude` is faster on this warm species query, but at materially higher RAM use.

#### Single-sample info

Query shape:
- sample = `SAMD00000692`

| Tool | Mode | Wall time | Max RSS |
|---|---|---:|---:|
| `atb-cli-codex` | warm, SQLite cache already built | 0.01 s | 18.9 MiB |
| `atb-cli-claude` | warm | 16.37 s | 2289.3 MiB |

Interpretation:
- `atb-cli-codex` is dramatically faster for exact-ID lookup once the SQLite cache exists.
- `atb-cli-claude` appears to scan parquet tables directly for `info`, which keeps implementation simple but is expensive for point lookups.

### Takeaways

- `atb-cli-codex` is currently optimized for repeated exact-ID lookups (`info`, `--sample-id`, `--genome-id`) through its SQLite query cache.
- `atb-cli-codex` still has a large first-run indexing cost that should be reduced or moved into `fetch`.
- `atb-cli-claude` currently wins on this warm species-wide query benchmark, but with much higher memory use.
- The two repos are making different tradeoffs right now: `atb-cli-codex` favors indexed reuse and lower steady-state RAM, while `atb-cli-claude` favors direct parquet scans and lower upfront setup cost.
