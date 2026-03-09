package grpclogger

import "testing"

func TestFindingSelectQueries(t *testing.T) {
	query := "SELECT 1"

	got := isSelectQuery(query)

	if !got {
		t.Fatalf("expected %v to be a select query", query)
	}

	query = "UPDATE x where y = 1"

	got = isSelectQuery(query)

	if got {
		t.Fatalf("expected %v to NOT be a select query", query)
	}
}

func TestFindingSelectQueriesWithParens(t *testing.T) {
	query := "((SELECT 1))"

	got := isSelectQuery(query)

	if !got {
		t.Fatalf("expected %v to be a select query", query)
	}
}

func TestFindingSelectQueriesWithParensAndWhitespace(t *testing.T) {
	query := "( SELECT 1 )"

	got := isSelectQuery(query)

	if !got {
		t.Fatalf("expected %v to be a select query", query)
	}
}

func TestFindingSelectQueriesButNotForUpdate(t *testing.T) {
	query := "SELECT x for update"

	got := isSelectQuery(query)

	if got {
		t.Fatalf("expected %v to NOT be a select query", query)
	}
}
