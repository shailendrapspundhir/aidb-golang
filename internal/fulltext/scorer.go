package fulltext

import "math"

// BM25Scorer computes BM25 relevance scores.
// BM25 formula:
//   score(d,q) = Σ IDF(qi) * (f(qi,d) * (k1 + 1)) / (f(qi,d) + k1 * (1 - b + b * |d| / avgdl))
//
// Where:
//   - f(qi,d) = term frequency of qi in document d
//   - |d| = length of document d
//   - avgdl = average document length in the collection
//   - k1 = term frequency saturation (typically 1.2-2.0)
//   - b = length normalization (typically 0.75)
//   - IDF(qi) = log((N - n(qi) + 0.5) / (n(qi) + 0.5) + 1)
type BM25Scorer struct {
	K1 float64 // term frequency saturation parameter
	B  float64 // length normalization parameter
}

// NewBM25Scorer creates a BM25 scorer with default parameters (k1=1.5, b=0.75).
func NewBM25Scorer() *BM25Scorer {
	return &BM25Scorer{K1: 1.5, B: 0.75}
}

// NewBM25ScorerWithParams creates a BM25 scorer with custom parameters.
func NewBM25ScorerWithParams(k1, b float64) *BM25Scorer {
	return &BM25Scorer{K1: k1, B: b}
}

// IDF computes the inverse document frequency for a term.
func (s *BM25Scorer) IDF(termDocCount, totalDocs int) float64 {
	if totalDocs == 0 || termDocCount == 0 {
		return 0
	}
	// Add 1 to avoid negative IDF; standard BM25+ variant
	n := float64(termDocCount)
	N := float64(totalDocs)
	return math.Log((N-n+0.5)/(n+0.5) + 1)
}

// Score computes the BM25 score for a single term in a document.
func (s *BM25Scorer) Score(termFreq, docLength int, idf, avgDocLength float64) float64 {
	if docLength == 0 {
		return 0
	}
	tf := float64(termFreq)
	dl := float64(docLength)
	norm := tf * (s.K1 + 1.0) / (tf + s.K1*(1.0-s.B+s.B*(dl/avgDocLength)))
	return idf * norm
}

// ScoredResult pairs a document ID with its relevance score.
type ScoredResult struct {
	DocID string
	Score float64
}

// ScoreDocuments computes BM25 scores for candidate documents.
// candidates: map of docID -> raw term frequency sum (from SearchTerms)
// idx: the inverted index (for doc lengths, total docs, avg len)
// queryTerms: the analyzed query terms
func (s *BM25Scorer) ScoreDocuments(
	candidates map[string]float64,
	idx *InvertedIndex,
	queryTerms []string,
) []ScoredResult {

	totalDocs := idx.TotalDocs()
	avgDL := idx.AvgDocLength()
	if totalDocs == 0 || avgDL == 0 {
		return nil
	}

	// Precompute IDF for each query term
	termIDF := make(map[string]float64)
	for _, term := range queryTerms {
		posts := idx.GetPostings(term)
		termIDF[term] = s.IDF(len(posts), totalDocs)
	}

	// Compute per-document scores
	docScores := make(map[string]float64)
	for docID := range candidates {
		docLen := idx.DocLength(docID)
		if docLen == 0 {
			continue
		}
		var totalScore float64
		for _, term := range queryTerms {
			posts := idx.GetPostings(term)
			// Find this doc's posting for this term
			var tf int
			for _, p := range posts {
				if p.DocID == docID {
					tf = p.TermFreq
					break
				}
			}
			if tf == 0 {
				continue
			}
			totalScore += s.Score(tf, docLen, termIDF[term], avgDL)
		}
		docScores[docID] = totalScore
	}

	// Convert to sorted slice
	results := make([]ScoredResult, 0, len(docScores))
	for docID, score := range docScores {
		results = append(results, ScoredResult{DocID: docID, Score: score})
	}

	// Sort descending by score
	for i := 0; i < len(results)-1; i++ {
		for j := i + 1; j < len(results); j++ {
			if results[j].Score > results[i].Score {
				results[i], results[j] = results[j], results[i]
			}
		}
	}

	return results
}
