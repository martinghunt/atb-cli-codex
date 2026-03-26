package cli

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/martin/atb-cli-codex/internal/cache"
	"github.com/martin/atb-cli-codex/internal/download"
	"github.com/martin/atb-cli-codex/internal/model"
	"github.com/martin/atb-cli-codex/internal/output"
	"github.com/martin/atb-cli-codex/internal/query"
	"github.com/martin/atb-cli-codex/internal/source"
	"github.com/martin/atb-cli-codex/internal/store"
	"github.com/spf13/cobra"
)

type rootOptions struct {
	cacheDir   string
	catalogURL string
}

func NewRootCommand(ctx context.Context) *cobra.Command {
	defaultCache, err := cache.DefaultRoot()
	if err != nil {
		defaultCache = ".atb-cache"
	}
	opts := rootOptions{
		cacheDir:   envOr("ATB_CACHE_DIR", defaultCache),
		catalogURL: os.Getenv("ATB_CATALOG_URL"),
	}
	cmd := &cobra.Command{
		Use:   "atb",
		Short: "Query and download AllTheBacteria data from a local managed cache.",
		Long: `atb is a user-friendly CLI for common AllTheBacteria workflows.

Use 'atb fetch' to populate the local cache, 'atb query' to search metadata,
'atb amr' or 'atb mlst' for common biological tasks, and 'atb download' to
plan or download genome FASTA files using AWS or OSF tarballs.`,
		SilenceUsage:  true,
		SilenceErrors: true,
	}
	cmd.PersistentFlags().StringVar(&opts.cacheDir, "cache-dir", opts.cacheDir, "Override the local ATB cache directory.")
	cmd.PersistentFlags().StringVar(&opts.catalogURL, "catalog-url", opts.catalogURL, "Source catalog URL used by fetch and update.")
	cmd.AddCommand(
		newFetchCommand(ctx, &opts),
		newUpdateCommand(ctx, &opts),
		newQueryCommand(ctx, &opts, "records"),
		newAMRCommand(ctx, &opts),
		newMLSTCommand(ctx, &opts),
		newInfoCommand(ctx, &opts),
		newStatsCommand(ctx, &opts),
		newDownloadCommand(ctx, &opts),
	)
	return cmd
}

func newFetchCommand(ctx context.Context, opts *rootOptions) *cobra.Command {
	var fetchMetadata bool
	var fetchAMR bool
	var genera []string
	cmd := &cobra.Command{
		Use:   "fetch",
		Short: "Download metadata and AMR cache files into the local ATB cache.",
		Long: `Fetch downloads cache files into the local ATB cache directory.

By default, fetch discovers the latest ATB metadata parquet release from the
canonical OSF project and discovers AMR parquet partitions from the canonical
GitHub repository. Use --catalog-url only if you need to override that with a
custom test or mirror catalog.`,
		Example: strings.Join([]string{
			"atb fetch --metadata",
			"atb fetch --amr --genus escherichia",
			"atb fetch --metadata --amr --catalog-url http://localhost:8080/catalog.json",
		}, "\n"),
		RunE: func(cmd *cobra.Command, _ []string) error {
			if !fetchMetadata && !fetchAMR {
				fetchMetadata, fetchAMR = true, true
			}
			syncer := source.Syncer{
				Layout:     cache.NewLayout(opts.cacheDir),
				Catalog:    catalogSource(cmd.ErrOrStderr(), opts, fetchMetadata, fetchAMR, genera),
				Downloader: source.HTTPDownloader{Logf: stderrLogger(cmd.ErrOrStderr())},
				Logf:       stderrLogger(cmd.ErrOrStderr()),
			}
			result, err := syncer.Fetch(ctx, fetchMetadata, fetchAMR, genera, false)
			if err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "Downloaded: %s\n", commaOrNone(result.Downloaded))
			fmt.Fprintf(cmd.OutOrStdout(), "Reused cached: %s\n", commaOrNone(result.Skipped))
			fmt.Fprintf(cmd.OutOrStdout(), "Metadata version: %s\nAMR version: %s\n", result.State.MetadataVersion, result.State.AMRVersion)
			return nil
		},
	}
	cmd.Flags().BoolVar(&fetchMetadata, "metadata", false, "Fetch metadata and manifest assets.")
	cmd.Flags().BoolVar(&fetchAMR, "amr", false, "Fetch AMR partition assets.")
	cmd.Flags().StringSliceVar(&genera, "genus", nil, "Limit AMR fetching to one or more genera.")
	return cmd
}

func newUpdateCommand(ctx context.Context, opts *rootOptions) *cobra.Command {
	var updateMetadata bool
	var updateAMR bool
	var genera []string
	cmd := &cobra.Command{
		Use:   "update",
		Short: "Refresh cached metadata and AMR assets and show the active cache state.",
		Long:  "Update refreshes cached ATB assets from the canonical OSF and GitHub sources, unless --catalog-url overrides discovery with a custom catalog.",
		Example: strings.Join([]string{
			"atb update --metadata",
			"atb update --amr --genus salmonella",
			"atb update --metadata --amr --catalog-url http://localhost:8080/catalog.json",
		}, "\n"),
		RunE: func(cmd *cobra.Command, _ []string) error {
			if !updateMetadata && !updateAMR {
				updateMetadata, updateAMR = true, true
			}
			syncer := source.Syncer{
				Layout:     cache.NewLayout(opts.cacheDir),
				Catalog:    catalogSource(cmd.ErrOrStderr(), opts, updateMetadata, updateAMR, genera),
				Downloader: source.HTTPDownloader{Logf: stderrLogger(cmd.ErrOrStderr())},
				Logf:       stderrLogger(cmd.ErrOrStderr()),
			}
			result, err := syncer.Fetch(ctx, updateMetadata, updateAMR, genera, true)
			if err != nil {
				return err
			}
			data, _ := json.MarshalIndent(result.State, "", "  ")
			fmt.Fprintln(cmd.OutOrStdout(), string(data))
			return nil
		},
	}
	cmd.Flags().BoolVar(&updateMetadata, "metadata", false, "Update metadata and manifest assets.")
	cmd.Flags().BoolVar(&updateAMR, "amr", false, "Update AMR partition assets.")
	cmd.Flags().StringSliceVar(&genera, "genus", nil, "Limit AMR updating to one or more genera.")
	return cmd
}

func newQueryCommand(ctx context.Context, opts *rootOptions, mode string) *cobra.Command {
	var q model.Query
	var format string
	var queryFile string
	var emitQuery bool
	var saveQuery string
	cmd := &cobra.Command{
		Use:   "query",
		Short: "Query ATB metadata using common species, ID, ST, and quality filters.",
		Long: `Query searches the local metadata cache and returns matching records.

Use direct flags for common work. For reproducibility, save the exact query as TOML
or load it again later with --query-file.`,
		Example: strings.Join([]string{
			`atb query --species "Escherichia coli" --sequence-type 131`,
			`atb query --species "Salmonella enterica" --limit 100 --sample-strategy even`,
			`atb query --species "Escherichia coli" --hq-only --format csv > ecoli.csv`,
			`atb query --query-file saved-query.toml --format json`,
		}, "\n"),
		RunE: func(cmd *cobra.Command, _ []string) error {
			if queryFile != "" {
				loaded, err := query.LoadQuery(queryFile)
				if err != nil {
					return fmt.Errorf("load query file: %w", err)
				}
				q = mergeQuery(q, loaded)
			}
			q.Mode = mode
			if saveQuery != "" {
				if err := query.SaveQuery(saveQuery, q); err != nil {
					return fmt.Errorf("save query: %w", err)
				}
			}
			if emitQuery {
				tomlText, err := query.EmitQueryTOML(q)
				if err != nil {
					return err
				}
				fmt.Fprint(cmd.OutOrStdout(), tomlText)
				return nil
			}
			rows, err := query.Service{Store: localStore(opts)}.Run(ctx, q)
			if err != nil {
				return err
			}
			if len(rows) == 0 {
				return fmt.Errorf("no matches found for the supplied filters")
			}
			return output.WriteRows(cmd.OutOrStdout(), model.OutputFormat(format), rows)
		},
	}
	addQueryFlags(cmd, &q, &format, &queryFile, &emitQuery, &saveQuery)
	return cmd
}

func newAMRCommand(ctx context.Context, opts *rootOptions) *cobra.Command {
	cmd := newQueryCommand(ctx, opts, "amr")
	cmd.Use = "amr"
	cmd.Short = "Return AMRFinderPlus-style hits for matching genomes."
	cmd.Long = "AMR is a thin wrapper over the shared query logic that resolves species filters to the needed genus-level AMR partition internally."
	cmd.Example = strings.Join([]string{
		`atb amr --species "Klebsiella pneumoniae" --hq-only`,
		`atb amr --species "Escherichia coli" --sequence-type 131 --format csv`,
	}, "\n")
	return cmd
}

func newMLSTCommand(ctx context.Context, opts *rootOptions) *cobra.Command {
	cmd := newQueryCommand(ctx, opts, "mlst")
	cmd.Use = "mlst"
	cmd.Short = "Return MLST summaries for matching genomes."
	cmd.Long = "MLST is a thin wrapper over the shared query logic and returns one row per matching sample with sequence type information."
	cmd.Example = strings.Join([]string{
		`atb mlst --species "Escherichia coli"`,
		`atb mlst --species "Salmonella enterica" --hq-only --format json`,
	}, "\n")
	return cmd
}

func newInfoCommand(ctx context.Context, opts *rootOptions) *cobra.Command {
	var id string
	var format string
	var includeENA bool
	cmd := &cobra.Command{
		Use:   "info",
		Short: "Show all available cached metadata for one sample or genome.",
		Long:  "Info returns a single metadata record using assembly metadata first. ENA tables are only scanned if --include-ena is requested.",
		Example: strings.Join([]string{
			"atb info --sample SAMPLE123",
			"atb info --sample SAMD00000344 --format json",
			"atb info --sample SAMD00000344 --include-ena --format json",
		}, "\n"),
		RunE: func(cmd *cobra.Command, _ []string) error {
			if id == "" {
				return fmt.Errorf("supply --sample with a sample or genome identifier")
			}
			row, err := query.Service{Store: localStore(opts)}.Info(ctx, id, includeENA)
			if err != nil {
				return err
			}
			return output.WriteRows(cmd.OutOrStdout(), model.OutputFormat(format), []map[string]any{row})
		},
	}
	cmd.Flags().StringVar(&id, "sample", "", "Sample or genome identifier to inspect.")
	cmd.Flags().BoolVar(&includeENA, "include-ena", false, "Also scan ENA parquet metadata for extra fields like country. This is slower and should only be used when you want ENA-derived fields.")
	cmd.Flags().StringVar(&format, "format", "tsv", "Output format: tsv, csv, json, or table. Default: tsv.")
	return cmd
}

func newStatsCommand(ctx context.Context, opts *rootOptions) *cobra.Command {
	var q model.Query
	var format string
	var queryFile string
	var emitQuery bool
	var saveQuery string
	cmd := &cobra.Command{
		Use:   "stats",
		Short: "Summarise the whole dataset or a filtered result set.",
		Long:  "Stats uses the same filters as query and reports the total genomes plus counts per species.",
		Example: strings.Join([]string{
			`atb stats`,
			`atb stats --species "Escherichia coli" --hq-only`,
			`atb stats --species "Salmonella enterica" --save-query salmonella.toml`,
		}, "\n"),
		RunE: func(cmd *cobra.Command, _ []string) error {
			if queryFile != "" {
				loaded, err := query.LoadQuery(queryFile)
				if err != nil {
					return fmt.Errorf("load query file: %w", err)
				}
				q = mergeQuery(q, loaded)
			}
			if saveQuery != "" {
				if err := query.SaveQuery(saveQuery, q); err != nil {
					return fmt.Errorf("save query: %w", err)
				}
			}
			if emitQuery {
				tomlText, err := query.EmitQueryTOML(q)
				if err != nil {
					return err
				}
				fmt.Fprint(cmd.OutOrStdout(), tomlText)
				return nil
			}
			stats, err := query.Service{Store: localStore(opts)}.Stats(ctx, q)
			if err != nil {
				return err
			}
			return output.WriteStats(cmd.OutOrStdout(), model.OutputFormat(format), stats)
		},
	}
	addQueryFlags(cmd, &q, &format, &queryFile, &emitQuery, &saveQuery)
	return cmd
}

func newDownloadCommand(ctx context.Context, opts *rootOptions) *cobra.Command {
	var input string
	var samples []string
	var strategy string
	var dryRun bool
	var printCommand bool
	cmd := &cobra.Command{
		Use:   "download",
		Short: "Plan or download genome FASTA files for sample IDs.",
		Long: `Download reads sample IDs from flags, a file, or stdin and chooses a download strategy.

Small requests default to AWS direct objects. Larger requests can use OSF tarballs
plus selective extraction. Use --dry-run or --print-command to inspect the plan.`,
		Example: strings.Join([]string{
			"atb download --sample SAMD00000344",
			"atb download --input ecoli.csv --print-command",
			"atb download --input salmonella.csv --strategy auto --dry-run",
		}, "\n"),
		RunE: func(cmd *cobra.Command, _ []string) error {
			ids, err := gatherSamples(cmd.InOrStdin(), input, samples)
			if err != nil {
				return err
			}
			manifest, err := localStore(opts).Assemblies(ctx)
			if err != nil {
				return err
			}
			layout := cache.NewLayout(opts.cacheDir)
			plan, err := download.PlanDownloads(ids, manifest, layout.Genomes, download.Strategy(strategy))
			if err != nil {
				return err
			}
			if dryRun {
				data, _ := json.MarshalIndent(plan, "", "  ")
				fmt.Fprintln(cmd.OutOrStdout(), string(data))
				return nil
			}
			if printCommand {
				fmt.Fprintln(cmd.OutOrStdout(), download.PlanCommand(plan))
				return nil
			}
			return download.Executor{}.Execute(ctx, plan, layout.Genomes)
		},
	}
	cmd.Flags().StringSliceVar(&samples, "sample", nil, "One or more sample IDs to download.")
	cmd.Flags().StringVar(&input, "input", "", "Read sample IDs from a file. CSV/TSV first column and plain text are supported.")
	cmd.Flags().StringVar(&strategy, "strategy", string(download.StrategyAuto), "Download strategy: auto, aws, or osf-tarball.")
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "Print the selected download plan without downloading.")
	cmd.Flags().BoolVar(&printCommand, "print-command", false, "Print equivalent curl commands instead of downloading.")
	return cmd
}

func addQueryFlags(cmd *cobra.Command, q *model.Query, format *string, queryFile *string, emitQuery *bool, saveQuery *string) {
	var sequenceType int
	var checkM2Min float64
	var checkM2MaxContamination float64
	cmd.Flags().StringVar(&q.Species, "species", "", "Exact species name filter, for example 'Escherichia coli'.")
	cmd.Flags().StringVar(&q.SampleID, "sample-id", "", "Filter to one sample ID.")
	cmd.Flags().StringVar(&q.GenomeID, "genome-id", "", "Filter to one genome ID.")
	cmd.Flags().StringVar(&q.ASMFASTAOnOSF, "asm-fasta-on-osf", "", "Filter on asm_fasta_on_osf: 1, 0, or any. Default: 1.")
	cmd.Flags().IntVar(&sequenceType, "sequence-type", 0, "Filter by MLST sequence type.")
	cmd.Flags().BoolVar(&q.HQOnly, "hq-only", false, "Restrict to high-quality genomes.")
	cmd.Flags().Float64Var(&checkM2Min, "checkm2-min", 0, "Minimum CheckM2 completeness.")
	cmd.Flags().Float64Var(&checkM2MaxContamination, "checkm2-max-contamination", 0, "Maximum CheckM2 contamination.")
	cmd.Flags().IntVar(&q.Limit, "limit", 0, "Maximum number of rows to return.")
	cmd.Flags().StringVar(&q.SampleStrategy, "sample-strategy", "all", "Sampling strategy when using --limit: all or even.")
	cmd.Flags().Int64Var(&q.Seed, "seed", 1, "Deterministic seed used for sampling.")
	cmd.Flags().StringVar(format, "format", "tsv", "Output format: tsv, csv, json, or table. Default: tsv.")
	cmd.Flags().StringVar(queryFile, "query-file", "", "Load query filters from a TOML file.")
	cmd.Flags().BoolVar(emitQuery, "emit-query-toml", false, "Print the effective query as TOML and exit.")
	cmd.Flags().StringVar(saveQuery, "save-query", "", "Write the effective query TOML to a file.")
	_ = cmd.RegisterFlagCompletionFunc("sample-strategy", func(*cobra.Command, []string, string) ([]string, cobra.ShellCompDirective) {
		return []string{"all", "even"}, cobra.ShellCompDirectiveNoFileComp
	})
	_ = cmd.RegisterFlagCompletionFunc("format", func(*cobra.Command, []string, string) ([]string, cobra.ShellCompDirective) {
		return []string{"table", "csv", "tsv", "json"}, cobra.ShellCompDirectiveNoFileComp
	})
	_ = cmd.RegisterFlagCompletionFunc("asm-fasta-on-osf", func(*cobra.Command, []string, string) ([]string, cobra.ShellCompDirective) {
		return []string{"1", "0", "any"}, cobra.ShellCompDirectiveNoFileComp
	})
	cmd.PreRun = func(cmd *cobra.Command, _ []string) {
		if cmd.Flags().Changed("sequence-type") {
			q.SequenceType = &sequenceType
		}
		if cmd.Flags().Changed("checkm2-min") {
			q.CheckM2Min = &checkM2Min
		}
		if cmd.Flags().Changed("checkm2-max-contamination") {
			q.CheckM2MaxContamination = &checkM2MaxContamination
		}
	}
}

func localStore(opts *rootOptions) store.LocalStore {
	return store.LocalStore{
		Layout: cache.NewLayout(opts.cacheDir),
		Logf:   stderrLogger(os.Stderr),
	}
}

func catalogSource(stderr io.Writer, opts *rootOptions, includeMetadata, includeAMR bool, genera []string) source.CatalogSource {
	if opts.catalogURL != "" {
		return source.HTTPCatalog{URL: opts.catalogURL}
	}
	return source.DefaultCatalog{
		IncludeMetadata: includeMetadata,
		IncludeAMR:      includeAMR,
		Genera:          genera,
		Logf:            stderrLogger(stderr),
	}
}

func stderrLogger(w io.Writer) func(string, ...any) {
	return func(format string, args ...any) {
		if w == nil {
			return
		}
		fmt.Fprintf(w, "atb: "+format+"\n", args...)
	}
}

func commaOrNone(items []string) string {
	if len(items) == 0 {
		return "none"
	}
	return strings.Join(items, ", ")
}

func envOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func mergeQuery(overrides, loaded model.Query) model.Query {
	if overrides.Species == "" {
		overrides.Species = loaded.Species
	}
	if overrides.SampleID == "" {
		overrides.SampleID = loaded.SampleID
	}
	if overrides.GenomeID == "" {
		overrides.GenomeID = loaded.GenomeID
	}
	if overrides.ASMFASTAOnOSF == "" {
		overrides.ASMFASTAOnOSF = loaded.ASMFASTAOnOSF
	}
	if overrides.SequenceType == nil {
		overrides.SequenceType = loaded.SequenceType
	}
	if !overrides.HQOnly {
		overrides.HQOnly = loaded.HQOnly
	}
	if overrides.CheckM2Min == nil {
		overrides.CheckM2Min = loaded.CheckM2Min
	}
	if overrides.CheckM2MaxContamination == nil {
		overrides.CheckM2MaxContamination = loaded.CheckM2MaxContamination
	}
	if overrides.Limit == 0 {
		overrides.Limit = loaded.Limit
	}
	if overrides.SampleStrategy == "all" && loaded.SampleStrategy != "" {
		overrides.SampleStrategy = loaded.SampleStrategy
	}
	if overrides.Seed == 1 && loaded.Seed != 0 {
		overrides.Seed = loaded.Seed
	}
	return overrides
}

func gatherSamples(stdin io.Reader, input string, samples []string) ([]string, error) {
	out := append([]string{}, samples...)
	if input != "" {
		data, err := os.ReadFile(input)
		if err != nil {
			return nil, fmt.Errorf("read input file: %w", err)
		}
		out = append(out, parseSampleText(string(data))...)
	}
	if file, ok := stdin.(*os.File); ok {
		if stat, err := file.Stat(); err == nil && stat.Mode()&os.ModeCharDevice == 0 {
			scanner := bufio.NewScanner(stdin)
			var b strings.Builder
			for scanner.Scan() {
				b.WriteString(scanner.Text())
				b.WriteByte('\n')
			}
			if err := scanner.Err(); err != nil {
				return nil, err
			}
			out = append(out, parseSampleText(b.String())...)
		}
	}
	return out, nil
}

func parseSampleText(text string) []string {
	var ids []string
	lines := strings.Split(strings.TrimSpace(text), "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		fields := strings.FieldsFunc(line, func(r rune) bool { return r == ',' || r == '\t' || r == ' ' })
		if len(fields) > 0 {
			ids = append(ids, fields[0])
		}
	}
	return ids
}
