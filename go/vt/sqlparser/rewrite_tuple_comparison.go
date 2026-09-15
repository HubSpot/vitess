package sqlparser

// ExpandTupleComparisons rewrites (a,b,c) > (x,y,z) into the nested
// OR/AND form: (a>x OR (a=x AND (b>y OR (b=y AND c>z)))).
// Applies to >, >=, <, <= only when LHS is all ColNames.
func ExpandTupleComparisons(stmt Statement) Statement {
	out := Rewrite(stmt, nil, func(cursor *Cursor) bool {
		cmp, ok := cursor.Node().(*ComparisonExpr)
		if !ok {
			return true
		}
		if expanded, changed := expandTupleCmp(cmp); changed {
			cursor.Replace(expanded)
		}
		return true
	})
	return out.(Statement)
}

func expandTupleCmp(cmp *ComparisonExpr) (Expr, bool) {
	left, lok := cmp.Left.(ValTuple)
	right, rok := cmp.Right.(ValTuple)
	if !lok || !rok || len(left) != len(right) || len(left) < 2 {
		return nil, false
	}
	switch cmp.Operator {
	case GreaterThanOp, GreaterEqualOp, LessThanOp, LessEqualOp:
	default:
		return nil, false
	}
	for _, e := range left {
		if _, ok := e.(*ColName); !ok {
			return nil, false
		}
	}
	return buildTupleExpansion(left, right, cmp.Operator), true
}

func buildTupleExpansion(cols, vals ValTuple, op ComparisonExprOperator) Expr {
	if len(cols) == 1 {
		return &ComparisonExpr{Left: cols[0], Right: vals[0], Operator: op}
	}
	strictOp := GreaterThanOp
	if op == LessThanOp || op == LessEqualOp {
		strictOp = LessThanOp
	}
	return &OrExpr{
		Left: &ComparisonExpr{Left: cols[0], Right: vals[0], Operator: strictOp},
		Right: &AndExpr{
			Left:  &ComparisonExpr{Left: cols[0], Right: vals[0], Operator: EqualOp},
			Right: buildTupleExpansion(cols[1:], vals[1:], op),
		},
	}
}
