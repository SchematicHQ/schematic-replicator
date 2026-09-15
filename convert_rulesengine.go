package main

import (
	"encoding/json"
	"fmt"

	"github.com/schematichq/rulesengine"
	sgre "github.com/schematichq/schematic-go/rulesengine"
)

// schematic-go v1.5.5 stopped depending on github.com/schematichq/rulesengine and
// carries its own copy of the wire model types, so its merge helpers
// (datastream.PartialCompany / datastream.PartialUser) no longer accept the types
// the replicator caches. The replicator keeps caching
// github.com/schematichq/rulesengine values — that package's JSON encoding is the
// Redis payload contract with every SDK's WASM engine — and converts at the
// boundary instead.
//
// Every JSON-tagged field of the two packages agrees except for these, checked
// against rulesengine v0.1.25 and schematic-go v1.5.8:
//
//	Company.CreditPostpaid  credit_postpaid,omitempty  | field absent
//	Company.CreditBalances  credit_balances            | credit_balances,omitempty
//	Company.Keys            keys                       | keys,omitempty
//	User.Keys               keys                       | keys,omitempty
//
// Conversion is therefore a JSON round-trip — which keeps working as fields are
// added to both sides — with the three maps assigned across directly. Letting JSON
// carry them would turn an empty-but-non-nil map into an absent key and then into
// nil, flipping the cached payload from `"keys":{}` to `"keys":null`. The Go types
// of those fields are identical on both sides, so a plain assignment is enough.

// toSDKCompany converts a cached company into schematic-go's company model.
func toSDKCompany(in *rulesengine.Company) (*sgre.Company, error) {
	if in == nil {
		return nil, nil
	}

	data, err := json.Marshal(in)
	if err != nil {
		return nil, fmt.Errorf("marshal company for conversion: %w", err)
	}

	out := &sgre.Company{}
	if err := json.Unmarshal(data, out); err != nil {
		return nil, fmt.Errorf("unmarshal company for conversion: %w", err)
	}

	out.CreditBalances = in.CreditBalances
	out.Keys = in.Keys

	return out, nil
}

// fromSDKCompany converts schematic-go's company model back into the cached one.
//
// TODO(SCH-7408): schematic-go's Company has no CreditPostpaid field, so the round
// trip drops it. That is not a regression — schematic-go's DeepCopyCompany has
// never carried the field either, so every partial company merge already clears it
// — and restoring it here would jump the sequencing SCH-7408 owns. Once
// schematic-go carries the field it starts flowing through with no change here.
func fromSDKCompany(in *sgre.Company) (*rulesengine.Company, error) {
	if in == nil {
		return nil, nil
	}

	data, err := json.Marshal(in)
	if err != nil {
		return nil, fmt.Errorf("marshal company for conversion: %w", err)
	}

	out := &rulesengine.Company{}
	if err := json.Unmarshal(data, out); err != nil {
		return nil, fmt.Errorf("unmarshal company for conversion: %w", err)
	}

	out.CreditBalances = in.CreditBalances
	out.Keys = in.Keys

	return out, nil
}

// toSDKUser converts a cached user into schematic-go's user model.
func toSDKUser(in *rulesengine.User) (*sgre.User, error) {
	if in == nil {
		return nil, nil
	}

	data, err := json.Marshal(in)
	if err != nil {
		return nil, fmt.Errorf("marshal user for conversion: %w", err)
	}

	out := &sgre.User{}
	if err := json.Unmarshal(data, out); err != nil {
		return nil, fmt.Errorf("unmarshal user for conversion: %w", err)
	}

	out.Keys = in.Keys

	return out, nil
}

// fromSDKUser converts schematic-go's user model back into the cached one.
func fromSDKUser(in *sgre.User) (*rulesengine.User, error) {
	if in == nil {
		return nil, nil
	}

	data, err := json.Marshal(in)
	if err != nil {
		return nil, fmt.Errorf("marshal user for conversion: %w", err)
	}

	out := &rulesengine.User{}
	if err := json.Unmarshal(data, out); err != nil {
		return nil, fmt.Errorf("unmarshal user for conversion: %w", err)
	}

	out.Keys = in.Keys

	return out, nil
}
