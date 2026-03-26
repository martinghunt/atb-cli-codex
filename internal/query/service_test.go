package query

import (
	"context"
	"fmt"
	"testing"

	"github.com/martin/atb-cli-codex/internal/model"
	"github.com/martin/atb-cli-codex/internal/store"
)

type fakeStore struct {
	records         []model.Record
	amrByGenus      map[string][]model.AMRHit
	recordByID      map[string]model.Record
	recordByIDCalls int
	recordsCalls    int
	queryRecords    []model.Record
	queryRecordsErr error
	queryCalls      int
}

func (f *fakeStore) Records(context.Context) ([]model.Record, error) {
	f.recordsCalls++
	return f.records, nil
}
func (f *fakeStore) Assemblies(context.Context) ([]model.AssemblyEntry, error) { return nil, nil }
func (f *fakeStore) State(context.Context) (model.State, error)                { return model.State{}, nil }
func (f *fakeStore) InfoRow(context.Context, string, bool) (map[string]any, error) {
	return map[string]any{"sample_id": "S1"}, nil
}
func (f *fakeStore) RecordByID(_ context.Context, id string) (model.Record, error) {
	f.recordByIDCalls++
	record, ok := f.recordByID[id]
	if !ok {
		return model.Record{}, fmt.Errorf("no record found for %q", id)
	}
	return record, nil
}
func (f *fakeStore) AMRByGenus(_ context.Context, genus string) ([]model.AMRHit, error) {
	return f.amrByGenus[genus], nil
}
func (f *fakeStore) QueryRecords(context.Context, model.Query) ([]model.Record, error) {
	f.queryCalls++
	if f.queryRecordsErr != nil {
		return nil, f.queryRecordsErr
	}
	if f.queryRecords != nil {
		return f.queryRecords, nil
	}
	return nil, store.ErrQueryUnsupported
}

func TestServiceFiltersAndSampling(t *testing.T) {
	st := 131
	store := &fakeStore{records: []model.Record{
		{SampleID: "S1", Species: "Escherichia coli", Genus: "Escherichia", SequenceType: 131, HQ: true, CheckM2Completeness: 99, CheckM2Contamination: 1},
		{SampleID: "S2", Species: "Escherichia coli", Genus: "Escherichia", SequenceType: 131, HQ: false, CheckM2Completeness: 90, CheckM2Contamination: 6},
		{SampleID: "S3", Species: "Escherichia coli", Genus: "Escherichia", SequenceType: 69, HQ: true, CheckM2Completeness: 98, CheckM2Contamination: 0.8},
		{SampleID: "S4", Species: "Salmonella enterica", Genus: "Salmonella", SequenceType: 11, HQ: true, CheckM2Completeness: 97, CheckM2Contamination: 0.2},
		{SampleID: "S5", Species: "Salmonella enterica", Genus: "Salmonella", SequenceType: 13, HQ: true, CheckM2Completeness: 97, CheckM2Contamination: 0.3},
		{SampleID: "S6", Species: "Salmonella enterica", Genus: "Salmonella", SequenceType: 13, HQ: true, CheckM2Completeness: 97, CheckM2Contamination: 0.4},
	}}
	svc := Service{Store: store}

	rows, err := svc.Run(context.Background(), model.Query{
		Species:                 "Escherichia coli",
		SequenceType:            &st,
		HQOnly:                  true,
		CheckM2MaxContamination: floatPtr(2),
	})
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if len(rows) != 1 || rows[0]["sample_id"] != "S1" {
		t.Fatalf("unexpected filtered rows: %#v", rows)
	}

	rows, err = svc.Run(context.Background(), model.Query{
		Species:        "Salmonella enterica",
		Limit:          2,
		SampleStrategy: "even",
		Seed:           7,
	})
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 sampled rows, got %d", len(rows))
	}
	if rows[0]["sample_id"] == rows[1]["sample_id"] {
		t.Fatalf("expected distinct sampled rows, got %#v", rows)
	}
}

func TestServiceAMRResolvesGenusPartition(t *testing.T) {
	svc := Service{Store: &fakeStore{
		records: []model.Record{
			{SampleID: "S1", Species: "Escherichia coli", Genus: "Escherichia", HQ: true},
			{SampleID: "S2", Species: "Salmonella enterica", Genus: "Salmonella", HQ: true},
		},
		amrByGenus: map[string][]model.AMRHit{
			"escherichia": {
				{SampleID: "S1", GeneSymbol: "blaCTX-M-15", DrugClass: "beta-lactam", Method: "AMRFinderPlus", AMRVersion: "v1"},
			},
		},
	}}

	rows, err := svc.Run(context.Background(), model.Query{Species: "Escherichia coli", Mode: "amr"})
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 AMR row, got %d", len(rows))
	}
	if got := rows[0]["gene_symbol"]; got != "blaCTX-M-15" {
		t.Fatalf("unexpected AMR gene: %v", got)
	}
}

func TestServiceUsesTargetedLookupForExactSampleQuery(t *testing.T) {
	store := &fakeStore{
		recordByID: map[string]model.Record{
			"S1": {SampleID: "S1", GenomeID: "R1", Species: "Escherichia coli", Genus: "Escherichia", HQ: true},
		},
	}
	svc := Service{Store: store}

	rows, err := svc.Run(context.Background(), model.Query{SampleID: "S1"})
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if len(rows) != 1 || rows[0]["sample_id"] != "S1" {
		t.Fatalf("unexpected rows: %#v", rows)
	}
	if store.recordByIDCalls != 1 {
		t.Fatalf("expected targeted RecordByID lookup, got %d calls", store.recordByIDCalls)
	}
	if store.recordsCalls != 0 {
		t.Fatalf("expected no full Records load, got %d calls", store.recordsCalls)
	}
}

func TestServiceUsesBroadQueryBackendWhenAvailable(t *testing.T) {
	store := &fakeStore{
		queryRecords: []model.Record{
			{SampleID: "S1", Species: "Escherichia coli", Genus: "Escherichia", HQ: true},
		},
	}
	svc := Service{Store: store}

	rows, err := svc.Run(context.Background(), model.Query{Species: "Escherichia coli"})
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if len(rows) != 1 || rows[0]["sample_id"] != "S1" {
		t.Fatalf("unexpected rows: %#v", rows)
	}
	if store.queryCalls != 1 {
		t.Fatalf("expected QueryRecords call, got %d", store.queryCalls)
	}
	if store.recordsCalls != 0 {
		t.Fatalf("expected no full Records load, got %d calls", store.recordsCalls)
	}
}

func TestServiceStatsIncludesExtendedSummaries(t *testing.T) {
	store := &fakeStore{records: []model.Record{
		{SampleID: "S1", Species: "Escherichia coli", Genus: "Escherichia", HQ: true, GenomeID: "R1", CheckM2Completeness: 99, CheckM2Contamination: 1},
		{SampleID: "S2", Species: "Escherichia coli", Genus: "Escherichia", HQ: false, CheckM2Completeness: 85, CheckM2Contamination: 6},
		{SampleID: "S3", Species: "Salmonella enterica", Genus: "Salmonella", HQ: true, CheckM2Completeness: 97, CheckM2Contamination: 0.2, Country: "UK"},
	}}
	svc := Service{Store: store}

	stats, err := svc.Stats(context.Background(), model.Query{})
	if err != nil {
		t.Fatalf("Stats returned error: %v", err)
	}
	if stats.Total != 3 || stats.HQ != 2 || stats.NonHQ != 1 {
		t.Fatalf("unexpected headline stats: %#v", stats)
	}
	if stats.PerSpecies["Escherichia coli"] != 2 || stats.PerGenus["Salmonella"] != 1 {
		t.Fatalf("unexpected grouped stats: %#v", stats)
	}
	if stats.CheckM2CompletenessGE90 != 2 || stats.CheckM2ContaminationLE5 != 2 {
		t.Fatalf("unexpected CheckM2 summaries: %#v", stats)
	}
	if len(stats.TopSpecies) == 0 || stats.TopSpecies[0].Name != "Escherichia coli" || stats.TopSpecies[0].Count != 2 {
		t.Fatalf("unexpected top species: %#v", stats.TopSpecies)
	}
	if len(stats.FieldCoverage) == 0 {
		t.Fatalf("expected field coverage stats")
	}
}

func floatPtr(v float64) *float64 { return &v }
