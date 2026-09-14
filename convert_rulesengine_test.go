package main

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/schematichq/rulesengine"
	"github.com/schematichq/rulesengine/typeconvert"
	"github.com/stretchr/testify/require"
)

// fullyPopulatedCompany is a company with every JSON-tagged field set, including
// the nested types the conversion round-trip has to carry: Rule with both
// Conditions and ConditionGroups, Trait, FeatureEntitlement with WarningTiers,
// CompanyMetric and Subscription.
//
// CreditPostpaid is deliberately left unset: schematic-go has no such field yet
// (see the TODO on fromSDKCompany), so it is not part of what the round trip
// promises to preserve.
func fullyPopulatedCompany() *rulesengine.Company {
	ts := time.Date(2026, 9, 14, 12, 30, 45, 123456789, time.UTC)
	basePlanID := "plan-base"
	flagID := "flag-1"
	eventSubtype := "api-request"
	metricValue := int64(100)
	metricPeriod := rulesengine.MetricPeriodCurrentMonth
	monthReset := rulesengine.MetricPeriodMonthResetBilling
	creditID := "credit-1"
	consumptionRate := 2.5
	allocation := int64(500)
	usage := int64(42)
	softLimit := int64(450)
	creditRemaining := 10.5
	creditReserved := 1.5
	creditSettled := 12.0
	creditTotal := 100.0
	creditUsed := 89.5
	eventName := "api_request"

	traitDefinition := &rulesengine.TraitDefinition{
		ID:             "trait-def-1",
		ComparableType: typeconvert.ComparableTypeString,
		EntityType:     rulesengine.EntityTypeCompany,
	}

	condition := &rulesengine.Condition{
		ID:                        "cond-1",
		AccountID:                 "account-1",
		EnvironmentID:             "env-1",
		ConditionType:             rulesengine.ConditionTypeTrait,
		Operator:                  typeconvert.ComparableOperatorEquals,
		ResourceIDs:               rulesengine.JSONSlice[string]{"res-1", "res-2"},
		EventSubtype:              &eventSubtype,
		MetricValue:               &metricValue,
		MetricPeriod:              &metricPeriod,
		MetricPeriodMonthReset:    &monthReset,
		CreditID:                  &creditID,
		ConsumptionRate:           &consumptionRate,
		TraitDefinition:           traitDefinition,
		TraitValue:                "enterprise",
		ComparisonTraitDefinition: traitDefinition,
	}

	return &rulesengine.Company{
		ID:                "company-1",
		AccountID:         "account-1",
		EnvironmentID:     "env-1",
		BasePlanID:        &basePlanID,
		BillingProductIDs: rulesengine.JSONSlice[string]{"billing-1", "billing-2"},
		CreditBalances:    map[string]float64{"credit-1": 10.5, "credit-2": 0},
		Entitlements: rulesengine.JSONSlice[*rulesengine.FeatureEntitlement]{
			{
				Allocation:      &allocation,
				ConsumptionRate: &consumptionRate,
				CreditID:        &creditID,
				CreditRemaining: &creditRemaining,
				CreditReserved:  &creditReserved,
				CreditSettled:   &creditSettled,
				CreditTotal:     &creditTotal,
				CreditUsed:      &creditUsed,
				EventName:       &eventName,
				EventSubtype:    &eventSubtype,
				FeatureID:       "feature-1",
				FeatureKey:      "feature-key-1",
				MetricPeriod:    &metricPeriod,
				MetricResetAt:   &ts,
				MonthReset:      &monthReset,
				SoftLimit:       &softLimit,
				Usage:           &usage,
				ValueType:       rulesengine.EntitlementValueTypeNumeric,
				WarningTiers: rulesengine.JSONSlice[*rulesengine.WarningTier]{
					{Key: "warn-80", Value: 400},
					{Key: "warn-90", Value: 450},
				},
			},
		},
		Keys: map[string]string{"id": "company-1", "domain": "example.com"},
		Metrics: rulesengine.CompanyMetricCollection{
			{
				AccountID:     "account-1",
				EnvironmentID: "env-1",
				CompanyID:     "company-1",
				EventSubtype:  eventSubtype,
				Period:        rulesengine.MetricPeriodCurrentMonth,
				MonthReset:    rulesengine.MetricPeriodMonthResetBilling,
				Value:         42,
				CreatedAt:     ts,
				ValidUntil:    &ts,
			},
		},
		PlanIDs:        rulesengine.JSONSlice[string]{"plan-1", "plan-2"},
		PlanVersionIDs: rulesengine.JSONSlice[string]{"plan-version-1"},
		Rules: rulesengine.JSONSlice[*rulesengine.Rule]{
			{
				ID:              "rule-1",
				FlagID:          &flagID,
				AccountID:       "account-1",
				EnvironmentID:   "env-1",
				RuleType:        rulesengine.RuleTypeCompanyOverride,
				Name:            "Company override",
				Priority:        1,
				Conditions:      rulesengine.JSONSlice[*rulesengine.Condition]{condition},
				ConditionGroups: rulesengine.JSONSlice[*rulesengine.ConditionGroup]{{Conditions: rulesengine.JSONSlice[*rulesengine.Condition]{condition}}},
				Value:           true,
			},
		},
		Subscription: &rulesengine.Subscription{
			ID:          "sub-1",
			PeriodStart: ts,
			PeriodEnd:   ts.Add(30 * 24 * time.Hour),
		},
		Traits: rulesengine.JSONSlice[*rulesengine.Trait]{
			{TraitDefinition: traitDefinition, Value: "enterprise"},
		},
	}
}

// fullyPopulatedUser is a user with every JSON-tagged field set.
func fullyPopulatedUser() *rulesengine.User {
	company := fullyPopulatedCompany()
	return &rulesengine.User{
		ID:            "user-1",
		AccountID:     "account-1",
		EnvironmentID: "env-1",
		Keys:          map[string]string{"id": "user-1", "email": "user@example.com"},
		Traits:        company.Traits,
		Rules:         company.Rules,
	}
}

// The Redis payload the replicator writes is read by every SDK's WASM engine, so
// its JSON shape is a contract: what goes through the conversion boundary has to
// serialize exactly as the rulesengine struct did before it.
func TestCompanyConversionRoundTripPreservesJSON(t *testing.T) {
	company := fullyPopulatedCompany()
	want, err := json.Marshal(company)
	require.NoError(t, err)

	sdkCompany, err := toSDKCompany(company)
	require.NoError(t, err)
	got, err := fromSDKCompany(sdkCompany)
	require.NoError(t, err)

	gotJSON, err := json.Marshal(got)
	require.NoError(t, err)
	require.Equal(t, string(want), string(gotJSON))
}

func TestUserConversionRoundTripPreservesJSON(t *testing.T) {
	user := fullyPopulatedUser()
	want, err := json.Marshal(user)
	require.NoError(t, err)

	sdkUser, err := toSDKUser(user)
	require.NoError(t, err)
	got, err := fromSDKUser(sdkUser)
	require.NoError(t, err)

	gotJSON, err := json.Marshal(got)
	require.NoError(t, err)
	require.Equal(t, string(want), string(gotJSON))
}

// The maps below are where the two packages' tags disagree: schematic-go marks
// them omitempty, so letting JSON carry them would turn an empty-but-non-nil map
// into an absent key and then into nil, flipping `{}` to `null` in the payload
// the SDKs read.
func TestCompanyConversionPreservesMapEmptiness(t *testing.T) {
	tests := []struct {
		name           string
		keys           map[string]string
		creditBalances map[string]float64
		wantJSON       []string
	}{
		{
			name:     "nil maps",
			wantJSON: []string{`"credit_balances":null`, `"keys":null`},
		},
		{
			name:           "empty non-nil maps",
			keys:           map[string]string{},
			creditBalances: map[string]float64{},
			wantJSON:       []string{`"credit_balances":{}`, `"keys":{}`},
		},
		{
			name:           "populated maps",
			keys:           map[string]string{"domain": "example.com"},
			creditBalances: map[string]float64{"credit-1": 1.5},
			wantJSON:       []string{`"credit_balances":{"credit-1":1.5}`, `"keys":{"domain":"example.com"}`},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			company := &rulesengine.Company{ID: "company-1", Keys: tt.keys, CreditBalances: tt.creditBalances}
			want, err := json.Marshal(company)
			require.NoError(t, err)
			for _, fragment := range tt.wantJSON {
				require.Contains(t, string(want), fragment)
			}

			sdkCompany, err := toSDKCompany(company)
			require.NoError(t, err)
			got, err := fromSDKCompany(sdkCompany)
			require.NoError(t, err)

			gotJSON, err := json.Marshal(got)
			require.NoError(t, err)
			require.Equal(t, string(want), string(gotJSON))
		})
	}
}

func TestUserConversionPreservesMapEmptiness(t *testing.T) {
	for _, tt := range []struct {
		name string
		keys map[string]string
	}{
		{name: "nil keys"},
		{name: "empty non-nil keys", keys: map[string]string{}},
		{name: "populated keys", keys: map[string]string{"email": "user@example.com"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			user := &rulesengine.User{ID: "user-1", Keys: tt.keys}
			want, err := json.Marshal(user)
			require.NoError(t, err)

			sdkUser, err := toSDKUser(user)
			require.NoError(t, err)
			got, err := fromSDKUser(sdkUser)
			require.NoError(t, err)

			gotJSON, err := json.Marshal(got)
			require.NoError(t, err)
			require.Equal(t, string(want), string(gotJSON))
		})
	}
}

func TestConversionHelpersHandleNil(t *testing.T) {
	sdkCompany, err := toSDKCompany(nil)
	require.NoError(t, err)
	require.Nil(t, sdkCompany)

	company, err := fromSDKCompany(nil)
	require.NoError(t, err)
	require.Nil(t, company)

	sdkUser, err := toSDKUser(nil)
	require.NoError(t, err)
	require.Nil(t, sdkUser)

	user, err := fromSDKUser(nil)
	require.NoError(t, err)
	require.Nil(t, user)
}

// Same contract as above, but through the cache write path against a real Redis
// (miniredis) rather than the helpers alone: the bytes stored under the company
// and user keys must be exactly what rulesengine v0.1.25 serializes.
func TestConvertedEntitiesStoreUnchangedRedisPayload(t *testing.T) {
	ctx := context.Background()
	mr, err := miniredis.Run()
	require.NoError(t, err)
	t.Cleanup(mr.Close)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})

	companies := NewRedisBatchCache[*rulesengine.Company](client, time.Minute)
	users := NewRedisBatchCache[*rulesengine.User](client, time.Minute)

	company := fullyPopulatedCompany()
	wantCompany, err := json.Marshal(company)
	require.NoError(t, err)

	sdkCompany, err := toSDKCompany(company)
	require.NoError(t, err)
	convertedCompany, err := fromSDKCompany(sdkCompany)
	require.NoError(t, err)

	companyKey := companyIDCacheKey(company.ID)
	require.NoError(t, companies.Set(ctx, companyKey, convertedCompany, time.Minute))
	storedCompany, err := mr.Get(companyKey)
	require.NoError(t, err)
	require.Equal(t, string(wantCompany), storedCompany)

	user := fullyPopulatedUser()
	wantUser, err := json.Marshal(user)
	require.NoError(t, err)

	sdkUser, err := toSDKUser(user)
	require.NoError(t, err)
	convertedUser, err := fromSDKUser(sdkUser)
	require.NoError(t, err)

	userKey := userIDCacheKey(user.ID)
	require.NoError(t, users.Set(ctx, userKey, convertedUser, time.Minute))
	storedUser, err := mr.Get(userKey)
	require.NoError(t, err)
	require.Equal(t, string(wantUser), storedUser)
}
