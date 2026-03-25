package indexes

import "github.com/blevesearch/bleve/v2/search/query"

func normalizeHybridFullTextQueryWithMode(q query.Query) (query.Query, HybridFullTextMode) {
	if q == nil {
		return nil, HybridFullTextModeNone
	}

	switch qt := q.(type) {
	case *query.MatchAllQuery:
		return qt, HybridFullTextModeMatchAll
	case *query.MatchNoneQuery:
		return qt, HybridFullTextModeMatchNone
	case *query.QueryStringQuery:
		parsed, err := qt.Parse()
		if err != nil {
			return qt, HybridFullTextModeNone
		}
		normalized, mode := normalizeHybridFullTextQueryWithMode(parsed)
		if mode != HybridFullTextModeNone {
			return normalized, mode
		}
		return qt, HybridFullTextModeNone
	case *query.ConjunctionQuery:
		conjuncts := make([]query.Query, 0, len(qt.Conjuncts))
		for _, child := range qt.Conjuncts {
			normalizedChild, mode := normalizeHybridFullTextQueryWithMode(child)
			if mode == HybridFullTextModeMatchNone {
				return query.NewMatchNoneQuery(), HybridFullTextModeMatchNone
			}
			if mode == HybridFullTextModeMatchAll {
				continue
			}
			if normalizedChild != nil {
				conjuncts = append(conjuncts, normalizedChild)
			}
		}
		switch len(conjuncts) {
		case 0:
			return query.NewMatchAllQuery(), HybridFullTextModeMatchAll
		case 1:
			return normalizeHybridFullTextQueryWithMode(conjuncts[0])
		default:
			normalized := query.NewConjunctionQuery(conjuncts)
			if qt.BoostVal != nil {
				normalized.SetBoost(qt.Boost())
			}
			return normalized, HybridFullTextModeNone
		}
	case *query.DisjunctionQuery:
		actualMin := int(qt.Min)
		if actualMin <= 0 {
			actualMin = 1
		}
		remaining := make([]query.Query, 0, len(qt.Disjuncts))
		matchAllCount := 0
		for _, child := range qt.Disjuncts {
			normalizedChild, mode := normalizeHybridFullTextQueryWithMode(child)
			switch mode {
			case HybridFullTextModeMatchAll:
				matchAllCount++
			case HybridFullTextModeMatchNone:
				continue
			default:
				if normalizedChild != nil {
					remaining = append(remaining, normalizedChild)
				}
			}
		}
		requiredRemaining := actualMin - matchAllCount
		if requiredRemaining <= 0 {
			return query.NewMatchAllQuery(), HybridFullTextModeMatchAll
		}
		if requiredRemaining > len(remaining) || len(remaining) == 0 {
			return query.NewMatchNoneQuery(), HybridFullTextModeMatchNone
		}
		if len(remaining) == 1 && requiredRemaining == 1 {
			return normalizeHybridFullTextQueryWithMode(remaining[0])
		}
		normalized := query.NewDisjunctionQuery(remaining)
		if requiredRemaining > 1 {
			normalized.SetMin(float64(requiredRemaining))
		}
		if qt.BoostVal != nil {
			normalized.SetBoost(qt.Boost())
		}
		return normalized, HybridFullTextModeNone
	case *query.BooleanQuery:
		must, mustMode := normalizeHybridFullTextQueryWithMode(qt.Must)
		should, shouldMode := normalizeHybridFullTextQueryWithMode(qt.Should)
		mustNot, mustNotMode := normalizeHybridFullTextQueryWithMode(qt.MustNot)
		filter, filterMode := normalizeHybridFullTextQueryWithMode(qt.Filter)

		if mustMode == HybridFullTextModeMatchAll || mustMode == HybridFullTextModeMatchNone {
			must = nil
		}
		if shouldMode == HybridFullTextModeMatchAll || shouldMode == HybridFullTextModeMatchNone {
			should = nil
		}
		if mustNotMode == HybridFullTextModeMatchAll {
			return query.NewMatchNoneQuery(), HybridFullTextModeMatchNone
		}
		if filterMode == HybridFullTextModeMatchNone {
			return query.NewMatchNoneQuery(), HybridFullTextModeMatchNone
		}
		if mustNotMode == HybridFullTextModeMatchNone {
			mustNot = nil
		}
		if must == nil && should == nil && mustNot == nil && filterMode == HybridFullTextModeMatchAll {
			return query.NewMatchAllQuery(), HybridFullTextModeMatchAll
		}
		if filterMode == HybridFullTextModeMatchAll {
			filter = nil
		}

		if must == nil && should == nil && mustNot == nil && filter == nil {
			return query.NewMatchNoneQuery(), HybridFullTextModeMatchNone
		}
		if must != nil && should == nil && mustNot == nil && filter == nil {
			return normalizeHybridFullTextQueryWithMode(must)
		}
		if must == nil && should != nil && mustNot == nil && filter == nil {
			return normalizeHybridFullTextQueryWithMode(should)
		}
		if must == nil && should == nil && mustNot == nil && filter != nil {
			return normalizeHybridFullTextQueryWithMode(filter)
		}

		normalized := &query.BooleanQuery{
			Must:    must,
			Should:  should,
			MustNot: mustNot,
			Filter:  filter,
		}
		if qt.BoostVal != nil {
			normalized.SetBoost(qt.Boost())
		}
		return normalized, HybridFullTextModeNone
	default:
		return qt, HybridFullTextModeNone
	}
}

func (q *Query) HybridFullTextFallbackQuery(fallback query.Query) query.Query {
	if q == nil || fallback == nil {
		return nil
	}

	fullTextQuery := fallback
	if q.FilterQuery != nil {
		fullTextQuery = query.NewConjunctionQuery([]query.Query{fullTextQuery, q.FilterQuery})
	}
	if q.ExclusionQuery != nil {
		fullTextQuery = query.NewBooleanQuery(
			[]query.Query{fullTextQuery},
			nil,
			[]query.Query{q.ExclusionQuery},
		)
	}

	return fullTextQuery
}
