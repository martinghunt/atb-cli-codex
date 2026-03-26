package model

type Record struct {
	SampleID              string   `json:"sample_id" toml:"sample_id"`
	GenomeID              string   `json:"genome_id" toml:"genome_id"`
	Species               string   `json:"species" toml:"species"`
	Genus                 string   `json:"genus" toml:"genus"`
	ASMFASTAOnOSF         int64    `json:"asm_fasta_on_osf" toml:"asm_fasta_on_osf"`
	SequenceType          int      `json:"sequence_type" toml:"sequence_type"`
	MLSTScheme            string   `json:"mlst_scheme" toml:"mlst_scheme"`
	HQ                    bool     `json:"hq" toml:"hq"`
	CheckM2Completeness   float64  `json:"checkm2_completeness" toml:"checkm2_completeness"`
	CheckM2Contamination  float64  `json:"checkm2_contamination" toml:"checkm2_contamination"`
	Country               string   `json:"country" toml:"country"`
	CollectionYear        int      `json:"collection_year" toml:"collection_year"`
	AMRGenes              []string `json:"amr_genes,omitempty" toml:"amr_genes"`
	MetadataVersion       string   `json:"metadata_version" toml:"metadata_version"`
	AssemblySourceVersion string   `json:"assembly_source_version" toml:"assembly_source_version"`
}

type AMRHit struct {
	SampleID   string `json:"sample_id" toml:"sample_id"`
	Species    string `json:"species" toml:"species"`
	DrugClass  string `json:"drug_class" toml:"drug_class"`
	GeneSymbol string `json:"gene_symbol" toml:"gene_symbol"`
	Method     string `json:"method" toml:"method"`
	Genus      string `json:"genus" toml:"genus"`
	AMRVersion string `json:"amr_version" toml:"amr_version"`
}

type AssemblyEntry struct {
	SampleID       string `json:"sample_id" toml:"sample_id"`
	TarballName    string `json:"tarball_name" toml:"tarball_name"`
	TarballURL     string `json:"tarball_url" toml:"tarball_url"`
	FileInTarball  string `json:"file_in_tarball" toml:"file_in_tarball"`
	AWSURL         string `json:"aws_url" toml:"aws_url"`
	AssemblySHA256 string `json:"assembly_sha256,omitempty" toml:"assembly_sha256"`
}

type Query struct {
	Species                 string   `toml:"species" json:"species"`
	SampleID                string   `toml:"sample_id" json:"sample_id"`
	GenomeID                string   `toml:"genome_id" json:"genome_id"`
	ASMFASTAOnOSF           string   `toml:"asm_fasta_on_osf" json:"asm_fasta_on_osf"`
	SequenceType            *int     `toml:"sequence_type" json:"sequence_type"`
	HQOnly                  bool     `toml:"hq_only" json:"hq_only"`
	CheckM2Min              *float64 `toml:"checkm2_min" json:"checkm2_min"`
	CheckM2MaxContamination *float64 `toml:"checkm2_max_contamination" json:"checkm2_max_contamination"`
	Limit                   int      `toml:"limit" json:"limit"`
	SampleStrategy          string   `toml:"sample_strategy" json:"sample_strategy"`
	Seed                    int64    `toml:"seed" json:"seed"`
	Mode                    string   `toml:"mode" json:"mode"`
}

type Stats struct {
	Total                   int             `json:"total"`
	PerSpecies              map[string]int  `json:"per_species"`
	PerGenus                map[string]int  `json:"per_genus"`
	HQ                      int             `json:"hq"`
	NonHQ                   int             `json:"non_hq"`
	CheckM2CompletenessGE90 int             `json:"checkm2_completeness_ge_90"`
	CheckM2ContaminationLE5 int             `json:"checkm2_contamination_le_5"`
	TopSpecies              []NamedCount    `json:"top_species"`
	FieldCoverage           []FieldCoverage `json:"field_coverage"`
}

type NamedCount struct {
	Name  string `json:"name"`
	Count int    `json:"count"`
}

type FieldCoverage struct {
	Field      string `json:"field"`
	Present    int    `json:"present"`
	Total      int    `json:"total"`
	Percentage int    `json:"percentage"`
}

type OutputFormat string

const (
	FormatTable OutputFormat = "table"
	FormatCSV   OutputFormat = "csv"
	FormatTSV   OutputFormat = "tsv"
	FormatJSON  OutputFormat = "json"
)

type State struct {
	MetadataVersion string            `json:"metadata_version"`
	AMRVersion      string            `json:"amr_version"`
	Sources         map[string]string `json:"sources"`
	UpdatedAt       string            `json:"updated_at"`
}
