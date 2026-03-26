# AGENTS.md

## Project goal
Build a **user-friendly Go CLI** for querying and downloading data from AllTheBacteria (ATB), with a strong focus on:

- clear command design
- reproducible queries
- excellent help text and examples
- safe defaults
- machine-readable outputs
- thorough automated tests

The tool should make common microbiology workflows easy for non-expert command line users while still supporting power users.

---

## Product principles

1. **User friendliness first**
   - Commands should be discoverable and readable.
   - Prefer intuitive subcommands and flags over requiring users to write config unless config is clearly beneficial.
   - Every command must have practical examples in `--help` output.
   - Errors must explain what went wrong and how to fix it.

2. **Reproducibility is a feature**
   - Queries should be saveable and rerunnable.
   - Where useful, support both flags and a TOML query file.
   - Output should include metadata about ATB data version and filters used.
   - Consider a way to emit the equivalent reproducible query spec.

3. **Safe, predictable behaviour**
   - Avoid surprising network actions during query execution.
   - Expensive actions should be explicit, but initial setup should still be easy.
   - Use dry-run modes where appropriate, especially for download workflows.

4. **Composable CLI design**
   - Commands should work well in shells and pipelines.
   - Tabular output should support CSV/TSV/JSON.
   - Human-readable summaries should be separate from machine-readable output when possible.

5. **Tests are mandatory**
   - Any new behaviour must include tests.
   - Parser logic, query planning, formatting, and HTTP/data access should all be testable.
   - Prefer dependency injection and small interfaces to enable fast unit tests.

---

## Canonical data sources

The CLI should be built around the following sources.

### Metadata parquet files
- Use the parquet files from the ATB Metadata OSF component: `https://osf.io/h7wzy/files/osfstorage`.
- The tool should download the required parquet files for the user and store them in a sensible local cache directory for reuse.
- The CLI should not force users to manually browse OSF to find files.
- The implementation should treat local metadata as a managed cache with explicit version awareness.

### AMR parquet files
- AMR parquet files should come from the `atb-amr-shiny` repository directory:
  `https://github.com/immem-hackathon-2025/atb-amr-shiny/tree/main/data/amr_by_genus`
- This directory is large and organised by genus. The CLI should hide that complexity from the user.
- Users should be able to request AMR data by species or genome/sample identifiers without needing to know how the files are partitioned.

### FASTA downloads for individual samples
- For individual FASTA downloads, prefer AWS as the default path.
- Follow the AllTheBacteria assemblies documentation for the AWS layout and URL conventions.
- The AWS pattern is appropriate when users ask for one genome or a small number of genomes.

### FASTA downloads for large batches
- For large downloads, the CLI may prefer OSF tarballs and extract only the requested genomes from them.
- Follow the AllTheBacteria OSF batch-download documentation and assembly file-list guidance.
- The tool should decide between AWS and OSF tarballs using clear, documented heuristics, with an override flag for advanced users.

---

## Local cache and storage requirements

The tool should manage downloads for the user and store them in sensible OS-appropriate locations.

Recommended approach:

- Use the user cache/state directories via Go's standard library where possible.
- Keep ATB data under a dedicated app directory, for example:
  - metadata/parquet files
  - AMR parquet files
  - downloaded file lists / manifests
  - downloaded genome FASTA files
  - extracted temporary files where needed
  - version/state metadata
- Support overrides such as:
  - `--cache-dir`
  - `ATB_CACHE_DIR`
- Reuse previously downloaded files whenever possible.
- Avoid re-downloading unchanged files.
- Track provenance so the CLI can tell users what source file/version a query used.
- Temporary extraction directories must be cleaned up safely.

A good default on Unix-like systems would be a cache path under the user's cache directory rather than cluttering the current working directory.

---

## User stories to optimize for

Primary workflows mentioned in planning:

- Get all AMRF+ results for a species.
- Restrict results by quality filters, such as high quality only.
- Get all MLST for a species.
- Get all ST131 *E. coli*.
- Get 100 evenly spread *Salmonella* genomes.
- Get one genome by sample/genome identifier.
- Get the closest genome to my genome. *(likely later phase / may be out of initial scope)*
- Get a bundle of results for a selected genome set.
- Return the command needed to download matching genomes.
- Get all info on a sample.
- Get all genomes for a species.
- Restrict searches by HQ, CheckM2, and related quality fields.
- Show high-level stats such as total genomes and counts per species.

Out-of-scope or later-phase ideas should be clearly separated from MVP.

---

## Suggested command model

Design around a compact, memorable CLI name such as `atb`.

Recommended commands for MVP:

- `atb fetch`
  - Discover and download required metadata parquet files into the local cache.
  - Support version pinning where possible.
  - Be able to fetch metadata only, AMR only, or both.

- `atb update`
  - Refresh local metadata / AMR cache or switch to a newer supported ATB version.
  - Show what is already cached and what changed.

- `atb query`
  - Query genomes/samples using flags and optionally `--query-file <toml>`.
  - Return records or IDs.
  - Support filters like species, lineage/ST, sample IDs, quality thresholds, HQ only, etc.

- `atb info`
  - Return all metadata for a single sample/genome.

- `atb stats`
  - Summarise matching records or the whole local index.

- `atb download`
  - Download genomes for IDs from stdin, file, or prior query output.
  - For small sets, default to AWS individual FASTA downloads.
  - For large sets, optionally use OSF tarballs and extract the requested genomes.
  - Support dry run and `--print-command` / `--script` output.

- `atb workflow` or `atb run`
  - One-shot query → summarise → download pipeline.
  - Must still preserve reproducibility and testability.

Potential topic-specific shortcuts can be added only if they are thin wrappers around `query` and clearly improve usability, for example:

- `atb amr`
- `atb mlst`

Avoid proliferating narrowly scoped commands unless they are clearly easier than filters.

---

## UX requirements

### Help and examples
Each command must provide:

- one-line purpose
- short explanation
- 3 to 6 realistic examples
- explanation of output columns / fields where relevant

Examples should cover common tasks such as:

```bash
atb query --species "Escherichia coli" --sequence-type 131
atb query --species "Salmonella enterica" --limit 100 --sample-strategy even
atb amr --species "Klebsiella pneumoniae" --hq-only
atb mlst --species "Escherichia coli"
atb info --sample SAMPLE123
atb query --species "Escherichia coli" --hq-only --format csv > ecoli.csv
atb download --input ecoli.csv --print-command
atb download --sample SAMD00000344
atb download --input salmonella.csv --strategy auto --dry-run
```

### Output design
Support explicit output modes:

- `table` for humans
- `csv` for pipelines
- `tsv` for shell use
- `json` for programmatic use

Defaults:

- human-facing commands may default to `table`
- script-oriented commands should document `--format csv/json`

### Error messages
Errors should be actionable, for example:

- explain unknown flag values
- suggest valid filter names
- distinguish “no matches found” from true failures
- show when a local database has not been fetched yet and exactly what command to run
- explain whether the CLI expected AWS direct download or OSF tarball extraction

### Reproducibility support
At least one of the following should exist:

- `--emit-query-toml`
- `--save-query <file>`
- `--show-repro-command`

Users should be able to rerun a query later against the same ATB version.

---

## Data/query behaviour guidelines

1. **Version awareness**
   - The CLI must track ATB metadata / parquet version in local state.
   - Query outputs should optionally include version metadata.
   - Cached files should be attributable to a source URL or manifest entry.

2. **Filter model**
   - Filters should be consistent across commands.
   - Prefer explicit flags like:
     - `--species`
     - `--sample-id`
     - `--genome-id`
     - `--sequence-type`
     - `--hq-only`
     - `--checkm2-min`
     - `--checkm2-max-contamination`
     - `--limit`
   - For advanced cases, allow a TOML query file.

3. **Sampling semantics**
   - If supporting “100 evenly spread Salmonella”, define “evenly spread” precisely.
   - Sampling algorithms must be deterministic with a seed option.
   - Document the strategy in help text.

4. **AMR/MLST ergonomics**
   - Common biological concepts should feel first-class.
   - Users should not need to know internal column names to request AMRfinderPlus or MLST results.
   - The CLI should resolve species to the relevant AMR parquet partition(s) internally.

5. **Download handoff**
   - Users should be able to go from query results to downloads with minimal friction.
   - A command that prints the exact download command/script is highly desirable.
   - Download strategy should be explicit and documented:
     - `auto`
     - `aws`
     - `osf-tarball`

6. **Performance and pragmatism**
   - Prefer lazy loading or targeted reads where feasible.
   - Do not require loading every AMR parquet partition to answer a simple species query if that can be avoided.
   - Cache manifests and indexes that make repeated queries faster.

---

## Technical implementation guidance

### Language and structure
- Use Go.
- Prefer a clean package layout, for example:
  - `cmd/`
  - `internal/cli/`
  - `internal/query/`
  - `internal/store/`
  - `internal/output/`
  - `internal/download/`
  - `internal/cache/`
  - `internal/source/`

### Recommended libraries
Choose stable, well-supported packages. Suitable options may include:

- `cobra` for CLI structure
- `viper` only if config needs justify it; avoid overcomplicating configuration
- Go stdlib where possible for CSV/JSON/HTTP/filesystem

### Architecture
- Keep domain/query logic separate from CLI wiring.
- Avoid placing business logic directly inside Cobra command handlers.
- Use interfaces for database/store access and download backends.
- Ensure all side effects are mockable.
- Separate source discovery from download execution.
- Keep download planning testable: input IDs in, plan out.

### Local state
Define a simple local config/state location for:

- current ATB database version
- local cache path
- remote source URLs/manifests
- cached metadata parquet files
- cached AMR parquet files
- downloaded assembly file lists

This should be easy to override in tests.

---

## Download strategy guidance

For genome FASTA downloads:

1. **Small request sets**
   - Default to AWS individual FASTA downloads.
   - Construct URLs/S3 object paths from sample IDs.
   - Support printing the equivalent `aws s3 cp --no-sign-request ...` or `wget ...` command.

2. **Large request sets**
   - Consider OSF tarball download plus extraction.
   - Use the assembly file list to map sample IDs to:
     - archive name
     - archive URL
     - filename inside archive
   - Group requested samples by tarball to minimise downloads.
   - Extract only the requested FASTA files where feasible.

3. **User control**
   - Provide an override like `--strategy aws|osf-tarball|auto`.
   - In `auto`, choose a documented threshold-based heuristic.
   - `--dry-run` should explain what would be downloaded and why that strategy was chosen.

---

## Testing requirements

Tests must be added for all new features.

### Minimum expected test coverage by area

1. **Command parsing tests**
   - Flags parse correctly.
   - Invalid flag combinations fail with useful errors.

2. **Query logic tests**
   - Species/ST/quality filters behave as expected.
   - Sampling logic is deterministic.
   - Empty result cases are handled cleanly.
   - AMR species-to-partition resolution is tested.

3. **Output tests**
   - CSV/JSON/table output is stable and correct.
   - Headers/field names remain intentional.

4. **Fetch/update tests**
   - Network/download logic uses mocks or test servers.
   - Version pinning and update selection are verified.
   - Cache reuse is verified.

5. **Download tests**
   - Dry run behaviour works.
   - Printed commands/scripts are correct.
   - Input from file/stdin/query results is supported.
   - AWS planning vs OSF tarball planning is tested.
   - Extraction planning from tarball manifests is tested.

6. **Golden tests where helpful**
   - Use golden files for help text and formatted outputs where they improve confidence.

### Testing style
- Prefer small, focused unit tests.
- Add integration-style tests for key end-to-end CLI flows.
- Avoid brittle tests that depend on live external services.
- Use test fixtures for small representative metadata tables and manifest files.
- Do not hit OSF, GitHub, AWS, or live ATB services in tests.

---

## MVP vs later phases

### MVP
- fetch/update local metadata
- fetch/update AMR parquet indexes needed for queries
- query by species, sample/genome ID, sequence type, and quality fields
- AMRfinderPlus result retrieval
- MLST retrieval
- info for a sample/genome
- summary stats
- download by IDs / prior query output
- automatic local cache reuse
- reproducible query support
- comprehensive tests

### Later phase / optional
- nearest genome to my genome
- richer comparative analysis
- advanced phylogenetic similarity workflows
- very large-scale distributed download orchestration

Do not let later-phase ideas complicate the MVP UX.

---

## Definition of done
A change is done only when:

- the command UX is coherent and documented
- help text includes realistic examples
- tests are added and passing
- output formats are intentional and stable
- errors are actionable
- reproducibility considerations are addressed
- caching/downloading behaviour is explicit and tested

---

## Notes from current ATB sources
- The AllTheBacteria assemblies documentation says assemblies can be downloaded from OSF, AWS, or ENA; batched assemblies are on OSF, while individual FASTA files for each sample are on AWS, making AWS the easiest route for specific samples.
- The same documentation describes the AWS naming scheme as `s3://allthebacteria-assemblies/<SAMPLE_ID>.fa.gz` and provides equivalent HTTPS object URLs.
- The assembly documentation also points users to a latest file list containing columns such as sample accession, tarball filename, URL, and filename within the tarball, which is the right basis for OSF tarball extraction planning.
- The OSF batch-download documentation explains that the Metadata component project identifier is `h7wzy`, and that bulk file lists can be generated or consumed from OSF-hosted TSV manifests.
- The `atb-amr-shiny` repository stores AMR parquet files under `data/amr_by_genus`, so the CLI should account for genus-level partitioning internally rather than exposing it to users.
