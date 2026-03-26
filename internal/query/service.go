package query

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"slices"
	"strings"

	"github.com/BurntSushi/toml"
	"github.com/martin/atb-cli-codex/internal/model"
	"github.com/martin/atb-cli-codex/internal/store"
)

type Service struct {
	Store store.Store
}

func (s Service) Run(ctx context.Context, q model.Query) ([]map[string]any, error) {
	if err := Validate(q); err != nil {
		return nil, err
	}
	switch q.Mode {
	case "", "records":
		records, err := s.filterRecords(ctx, q)
		if err != nil {
			return nil, err
		}
		return recordsToRows(records), nil
	case "amr":
		return s.amrRows(ctx, q)
	case "mlst":
		records, err := s.filterRecords(ctx, q)
		if err != nil {
			return nil, err
		}
		rows := make([]map[string]any, 0, len(records))
		for _, rec := range records {
			rows = append(rows, map[string]any{
				"sample_id":     rec.SampleID,
				"species":       rec.Species,
				"sequence_type": rec.SequenceType,
				"mlst_scheme":   rec.MLSTScheme,
			})
		}
		return rows, nil
	default:
		return nil, fmt.Errorf("unsupported query mode %q", q.Mode)
	}
}

func (s Service) Info(ctx context.Context, id string, includeENA bool) (map[string]any, error) {
	return s.Store.InfoRow(ctx, id, includeENA)
}

func (s Service) Stats(ctx context.Context, q model.Query) (model.Stats, error) {
	records, err := s.filterRecords(ctx, q)
	if err != nil {
		return model.Stats{}, err
	}
	stats := model.Stats{Total: len(records), PerSpecies: map[string]int{}}
	for _, rec := range records {
		stats.PerSpecies[rec.Species]++
	}
	return stats, nil
}

func Validate(q model.Query) error {
	if q.Limit < 0 {
		return fmt.Errorf("--limit must be zero or greater")
	}
	if q.SampleStrategy != "" && q.SampleStrategy != "all" && q.SampleStrategy != "even" {
		return fmt.Errorf("unsupported --sample-strategy %q; valid values are all or even", q.SampleStrategy)
	}
	if q.SampleStrategy == "even" && q.Limit == 0 {
		return fmt.Errorf("--sample-strategy even requires --limit")
	}
	return nil
}

func (s Service) filterRecords(ctx context.Context, q model.Query) ([]model.Record, error) {
	if q.SampleID != "" || q.GenomeID != "" {
		id := q.SampleID
		if q.GenomeID != "" {
			id = q.GenomeID
		}
		record, err := s.Store.RecordByID(ctx, id)
		if err == nil {
			filtered := applyRecordFilters([]model.Record{record}, q)
			if q.Limit > 0 && len(filtered) > q.Limit {
				filtered = filtered[:q.Limit]
			}
			return filtered, nil
		}
		if !strings.Contains(err.Error(), "no record found") {
			return nil, err
		}
		return nil, nil
	}
	records, err := s.Store.Records(ctx)
	if err != nil {
		return nil, err
	}
	filtered := applyRecordFilters(records, q)
	if q.SampleStrategy == "even" {
		filtered = evenSample(filtered, q.Limit, q.Seed)
	} else if q.Limit > 0 && len(filtered) > q.Limit {
		filtered = filtered[:q.Limit]
	}
	return filtered, nil
}

func applyRecordFilters(records []model.Record, q model.Query) []model.Record {
	filtered := make([]model.Record, 0, len(records))
	for _, rec := range records {
		if q.Species != "" && !strings.EqualFold(rec.Species, q.Species) {
			continue
		}
		if q.SampleID != "" && rec.SampleID != q.SampleID {
			continue
		}
		if q.GenomeID != "" && rec.GenomeID != q.GenomeID {
			continue
		}
		if q.SequenceType != nil && rec.SequenceType != *q.SequenceType {
			continue
		}
		if q.HQOnly && !rec.HQ {
			continue
		}
		if q.CheckM2Min != nil && rec.CheckM2Completeness < *q.CheckM2Min {
			continue
		}
		if q.CheckM2MaxContamination != nil && rec.CheckM2Contamination > *q.CheckM2MaxContamination {
			continue
		}
		filtered = append(filtered, rec)
	}
	return filtered
}

func evenSample(records []model.Record, limit int, seed int64) []model.Record {
	if limit <= 0 || len(records) <= limit {
		return records
	}
	buckets := map[string][]model.Record{}
	for _, rec := range records {
		key := rec.MLSTScheme
		if rec.SequenceType > 0 {
			key = fmt.Sprintf("ST%d", rec.SequenceType)
		}
		if key == "" {
			key = rec.SampleID
		}
		buckets[key] = append(buckets[key], rec)
	}
	keys := make([]string, 0, len(buckets))
	for key := range buckets {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	rng := rand.New(rand.NewSource(seed))
	for _, key := range keys {
		rng.Shuffle(len(buckets[key]), func(i, j int) {
			buckets[key][i], buckets[key][j] = buckets[key][j], buckets[key][i]
		})
	}
	var out []model.Record
	for len(out) < limit {
		progress := false
		for _, key := range keys {
			if len(out) == limit {
				break
			}
			if len(buckets[key]) == 0 {
				continue
			}
			out = append(out, buckets[key][0])
			buckets[key] = buckets[key][1:]
			progress = true
		}
		if !progress {
			break
		}
	}
	slices.SortFunc(out, func(a, b model.Record) int { return strings.Compare(a.SampleID, b.SampleID) })
	return out
}

func (s Service) amrRows(ctx context.Context, q model.Query) ([]map[string]any, error) {
	records, err := s.filterRecords(ctx, q)
	if err != nil {
		return nil, err
	}
	bySample := map[string]model.Record{}
	seenGenera := map[string]bool{}
	for _, rec := range records {
		bySample[rec.SampleID] = rec
		seenGenera[strings.ToLower(rec.Genus)] = true
	}
	rows := []map[string]any{}
	for genus := range seenGenera {
		hits, err := s.Store.AMRByGenus(ctx, genus)
		if err != nil {
			return nil, err
		}
		for _, hit := range hits {
			rec, ok := bySample[hit.SampleID]
			if !ok {
				continue
			}
			rows = append(rows, map[string]any{
				"sample_id":     hit.SampleID,
				"species":       rec.Species,
				"gene_symbol":   hit.GeneSymbol,
				"drug_class":    hit.DrugClass,
				"method":        hit.Method,
				"amr_version":   hit.AMRVersion,
				"sequence_type": rec.SequenceType,
				"hq":            rec.HQ,
			})
		}
	}
	slices.SortFunc(rows, func(a, b map[string]any) int {
		if c := strings.Compare(fmt.Sprint(a["sample_id"]), fmt.Sprint(b["sample_id"])); c != 0 {
			return c
		}
		return strings.Compare(fmt.Sprint(a["gene_symbol"]), fmt.Sprint(b["gene_symbol"]))
	})
	if q.Limit > 0 && len(rows) > q.Limit {
		rows = rows[:q.Limit]
	}
	return rows, nil
}

func recordToRow(record model.Record) map[string]any {
	return map[string]any{
		"sample_id":               record.SampleID,
		"genome_id":               record.GenomeID,
		"species":                 record.Species,
		"genus":                   record.Genus,
		"sequence_type":           record.SequenceType,
		"mlst_scheme":             record.MLSTScheme,
		"hq":                      record.HQ,
		"checkm2_completeness":    record.CheckM2Completeness,
		"checkm2_contamination":   record.CheckM2Contamination,
		"country":                 record.Country,
		"collection_year":         record.CollectionYear,
		"amr_genes":               strings.Join(record.AMRGenes, ","),
		"metadata_version":        record.MetadataVersion,
		"assembly_source_version": record.AssemblySourceVersion,
	}
}

func recordsToRows(records []model.Record) []map[string]any {
	rows := make([]map[string]any, 0, len(records))
	for _, record := range records {
		rows = append(rows, recordToRow(record))
	}
	return rows
}

func SaveQuery(path string, q model.Query) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return toml.NewEncoder(f).Encode(q)
}

func LoadQuery(path string) (model.Query, error) {
	var q model.Query
	_, err := toml.DecodeFile(path, &q)
	return q, err
}

func EmitQueryTOML(q model.Query) (string, error) {
	var b strings.Builder
	if err := toml.NewEncoder(&b).Encode(q); err != nil {
		return "", err
	}
	return b.String(), nil
}
