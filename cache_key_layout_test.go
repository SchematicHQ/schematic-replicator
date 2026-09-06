package main

import (
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/schematichq/rulesengine"
	"github.com/stretchr/testify/require"
)

// redisKeyLayoutFixture mirrors testdata/redis_key_layout.json. The SDKs read
// the keys this process writes, so the fixture is the contract between them:
// every SDK has a unit test against the same cases. If a builder here changes,
// the fixture changes with it, and the SDK tests fail until they follow.
type redisKeyLayoutFixture struct {
	Prefix string `json:"prefix"`
	Cases  []struct {
		Kind  string            `json:"kind"`
		Input map[string]string `json:"input"`
		Key   string            `json:"key"`
	} `json:"cases"`
}

func TestRedisKeyLayoutMatchesFixture(t *testing.T) {
	raw, err := os.ReadFile("testdata/redis_key_layout.json")
	require.NoError(t, err)

	var fixture redisKeyLayoutFixture
	require.NoError(t, json.Unmarshal(raw, &fixture))
	require.Equal(t, cacheKeyPrefix, fixture.Prefix)
	require.NotEmpty(t, fixture.Cases)

	for _, c := range fixture.Cases {
		t.Run(c.Kind, func(t *testing.T) {
			want := strings.ReplaceAll(c.Key, "<VERSION>", rulesengine.VersionKey)
			var got string
			switch c.Kind {
			case "flag":
				got = flagCacheKey(c.Input["key"])
			case "company_id":
				got = companyIDCacheKey(c.Input["id"])
			case "company_lookup":
				got = resourceKeyToCacheKey(cacheKeyPrefixCompany, c.Input["key"], c.Input["value"])
			case "user_id":
				got = userIDCacheKey(c.Input["id"])
			case "user_lookup":
				got = resourceKeyToCacheKey(cacheKeyPrefixUser, c.Input["key"], c.Input["value"])
			default:
				t.Fatalf("fixture case kind %q has no builder here; add one or fix the fixture", c.Kind)
			}
			require.Equal(t, want, got)
		})
	}
}
