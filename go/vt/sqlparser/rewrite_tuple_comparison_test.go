package sqlparser

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExpandTupleComparisons(t *testing.T) {
	tests := []struct {
		name     string
		in       string
		expected string
	}{{
		name:     "two columns greater than",
		in:       "select * from t where (a, b) > (1, 2)",
		expected: "select * from t where a > 1 or a = 1 and b > 2",
	}, {
		name:     "two columns less than",
		in:       "select * from t where (a, b) < (1, 2)",
		expected: "select * from t where a < 1 or a = 1 and b < 2",
	}, {
		name:     "two columns greater equal",
		in:       "select * from t where (a, b) >= (1, 2)",
		expected: "select * from t where a > 1 or a = 1 and b >= 2",
	}, {
		name:     "two columns less equal",
		in:       "select * from t where (a, b) <= (1, 2)",
		expected: "select * from t where a < 1 or a = 1 and b <= 2",
	}, {
		name:     "three columns greater than",
		in:       "select * from t where (a, b, c) > (1, 2, 3)",
		expected: "select * from t where a > 1 or a = 1 and (b > 2 or b = 2 and c > 3)",
	}, {
		name:     "three columns less equal",
		in:       "select * from t where (a, b, c) <= (1, 2, 3)",
		expected: "select * from t where a < 1 or a = 1 and (b < 2 or b = 2 and c <= 3)",
	}, {
		name:     "equality not expanded",
		in:       "select * from t where (a, b) = (1, 2)",
		expected: "select * from t where (a, b) = (1, 2)",
	}, {
		name:     "non-column LHS not expanded",
		in:       "select * from t where (a + 1, b) > (1, 2)",
		expected: "select * from t where (a + 1, b) > (1, 2)",
	}, {
		name:     "single element tuple not expanded",
		in:       "select * from t where (a) > (1)",
		expected: "select * from t where a > 1",
	}, {
		name:     "non-tuple comparison untouched",
		in:       "select * from t where a > 1",
		expected: "select * from t where a > 1",
	}, {
		name:     "mixed conditions",
		in:       "select * from t where x = 1 and (a, b) > (1, 2)",
		expected: "select * from t where x = 1 and (a > 1 or a = 1 and b > 2)",
	}, {
		name:     "qualified column names",
		in:       "select * from t where (t.a, t.b) > (1, 2)",
		expected: "select * from t where t.a > 1 or t.a = 1 and t.b > 2",
	}}

	parser := NewTestParser()
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			stmt, err := parser.Parse(tc.in)
			require.NoError(t, err)

			result := ExpandTupleComparisons(stmt)
			assert.Equal(t, tc.expected, String(result))
		})
	}
}
