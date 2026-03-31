package fulltext

import (
	"regexp"
	"sync"
)

// Posting represents a single occurrence of a term in a document.
type Posting struct {
	DocID     string
	TermFreq  int
	Positions []int // optional positions for phrase search
}

// InvertedIndex stores term -> postings mappings for full-text search.
type InvertedIndex struct {
	mu          sync.RWMutex
	postings    map[string][]Posting // term -> postings list
	docLengths  map[string]int       // docID -> number of terms
	totalDocs   int
	totalTerms  int64 // total terms across all docs (for avg doc len)
	analyzer    *Analyzer
}

// NewInvertedIndex creates a new inverted index.
func NewInvertedIndex() *InvertedIndex {
	return &InvertedIndex{
		postings:   make(map[string][]Posting),
		docLengths: make(map[string]int),
		analyzer:   StandardAnalyzer(),
	}
}

// SetAnalyzer sets a custom analyzer for indexing.
func (idx *InvertedIndex) SetAnalyzer(a *Analyzer) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	idx.analyzer = a
}

// IndexDocument analyzes and indexes a document's text content.
// fieldValue is the text content; docID is the document identifier.
func (idx *InvertedIndex) IndexDocument(docID string, fieldValues []string) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	// Remove existing postings for this doc (reindex)
	idx.removeDocLocked(docID)

	allTerms := make([]string, 0)
	for _, fv := range fieldValues {
		terms := idx.analyzer.Analyze(fv)
		allTerms = append(allTerms, terms...)
	}

	if len(allTerms) == 0 {
		return
	}

	// Build term -> positions map for this doc
	termPositions := make(map[string][]int)
	for i, term := range allTerms {
		termPositions[term] = append(termPositions[term], i)
	}

	// Create postings
	for term, positions := range termPositions {
		p := Posting{
			DocID:     docID,
			TermFreq:  len(positions),
			Positions: positions,
		}
		idx.postings[term] = append(idx.postings[term], p)
	}

	idx.docLengths[docID] = len(allTerms)
	idx.totalDocs++
	idx.totalTerms += int64(len(allTerms))
}

// RemoveDocument removes all postings for a document.
func (idx *InvertedIndex) RemoveDocument(docID string) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	idx.removeDocLocked(docID)
}

func (idx *InvertedIndex) removeDocLocked(docID string) {
	docLen, exists := idx.docLengths[docID]
	if !exists {
		return
	}

	// Remove postings for this doc from all terms
	for term, posts := range idx.postings {
		newPosts := make([]Posting, 0, len(posts))
		for _, p := range posts {
			if p.DocID != docID {
				newPosts = append(newPosts, p)
			}
		}
		if len(newPosts) == 0 {
			delete(idx.postings, term)
		} else {
			idx.postings[term] = newPosts
		}
	}

	delete(idx.docLengths, docID)
	idx.totalDocs--
	idx.totalTerms -= int64(docLen)
}

// GetPostings returns all postings for a term.
func (idx *InvertedIndex) GetPostings(term string) []Posting {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	posts, ok := idx.postings[term]
	if !ok {
		return nil
	}
	// Return a copy to avoid race
	result := make([]Posting, len(posts))
	copy(result, posts)
	return result
}

// DocLength returns the number of terms in a document.
func (idx *InvertedIndex) DocLength(docID string) int {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.docLengths[docID]
}

// TotalDocs returns total number of documents indexed.
func (idx *InvertedIndex) TotalDocs() int {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.totalDocs
}

// AvgDocLength returns average document length.
func (idx *InvertedIndex) AvgDocLength() float64 {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	if idx.totalDocs == 0 {
		return 0
	}
	return float64(idx.totalTerms) / float64(idx.totalDocs)
}

// Terms returns all indexed terms.
func (idx *InvertedIndex) Terms() []string {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	terms := make([]string, 0, len(idx.postings))
	for t := range idx.postings {
		terms = append(terms, t)
	}
	return terms
}

// Clear removes all data from the index.
func (idx *InvertedIndex) Clear() {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	idx.postings = make(map[string][]Posting)
	idx.docLengths = make(map[string]int)
	idx.totalDocs = 0
	idx.totalTerms = 0
}

// SearchTerms returns candidate document IDs that contain any of the terms.
// This is a basic OR search; scoring is done separately.
func (idx *InvertedIndex) SearchTerms(terms []string) map[string]float64 {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	scores := make(map[string]float64)
	for _, term := range terms {
		posts, ok := idx.postings[term]
		if !ok {
			continue
		}
		for _, p := range posts {
			scores[p.DocID] += float64(p.TermFreq)
		}
	}
	return scores
}

// PhraseSearch finds documents where terms appear in exact sequence.
// Terms must be adjacent (position difference of 1) in the given order.
func (idx *InvertedIndex) PhraseSearch(terms []string) map[string]float64 {
	if len(terms) == 0 {
		return nil
	}
	if len(terms) == 1 {
		// Single term - just return those docs
		return idx.SearchTerms(terms)
	}

	idx.mu.RLock()
	defer idx.mu.RUnlock()

	// Start with docs containing the first term
	firstPosts, ok := idx.postings[terms[0]]
	if !ok {
		return nil
	}

	// Track which docs have valid phrase matches
	// We'll check each subsequent term's positions
	result := make(map[string]float64)

	for _, firstP := range firstPosts {
		docID := firstP.DocID
		// For each position of the first term, check if phrase continues
		for _, startPos := range firstP.Positions {
			if idx.phraseMatchFrom(docID, terms, startPos) {
				// Count phrase occurrences (simplified: increment per starting position)
				result[docID] += 1
			}
		}
	}

	return result
}

// phraseMatchFrom checks if terms[1:] appear at consecutive positions starting from startPos+1
func (idx *InvertedIndex) phraseMatchFrom(docID string, terms []string, startPos int) bool {
	currentPos := startPos
	for i := 1; i < len(terms); i++ {
		posts, ok := idx.postings[terms[i]]
		if !ok {
			return false
		}
		// Find posting for this doc
		found := false
		for _, p := range posts {
			if p.DocID == docID {
				// Check if currentPos+1 exists in this term's positions
				for _, pos := range p.Positions {
					if pos == currentPos+1 {
						currentPos = pos
						found = true
						break
					}
				}
				if found {
					break
				}
			}
		}
		if !found {
			return false
		}
	}
	return true
}

// FuzzySearch finds documents containing terms within maxEditDistance of query terms.
// Uses Levenshtein distance. Returns docID -> score (based on matching term count).
func (idx *InvertedIndex) FuzzySearch(queryTerms []string, maxEditDistance int) map[string]float64 {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if maxEditDistance < 0 {
		maxEditDistance = 2
	}

	result := make(map[string]float64)

	for _, qTerm := range queryTerms {
		// Find all indexed terms within edit distance
		for term, posts := range idx.postings {
			if levenshtein(qTerm, term) <= maxEditDistance {
				for _, p := range posts {
					result[p.DocID] += float64(p.TermFreq)
				}
			}
		}
	}

	return result
}

// levenshtein computes the Levenshtein edit distance between two strings.
func levenshtein(a, b string) int {
	if len(a) == 0 {
		return len(b)
	}
	if len(b) == 0 {
		return len(a)
	}

	// Optimize by using the shorter string for the DP array
	if len(a) < len(b) {
		a, b = b, a
	}

	// Previous row of distances
	previous := make([]int, len(b)+1)
	current := make([]int, len(b)+1)

	// Initialize previous row
	for j := 0; j <= len(b); j++ {
		previous[j] = j
	}

	for i := 1; i <= len(a); i++ {
		current[0] = i
		for j := 1; j <= len(b); j++ {
			cost := 0
			if a[i-1] != b[j-1] {
				cost = 1
			}
			current[j] = min(
				previous[j]+1,      // deletion
				current[j-1]+1,     // insertion
				previous[j-1]+cost, // substitution
			)
		}
		previous, current = current, previous
	}

	return previous[len(b)]
}

func min(a, b, c int) int {
	if a <= b && a <= c {
		return a
	}
	if b <= a && b <= c {
		return b
	}
	return c
}

// RegexSearch finds documents where any indexed term matches the given regex pattern.
// Returns a map of docID -> score (number of matching terms as a simple score).
func (idx *InvertedIndex) RegexSearch(pattern string) map[string]float64 {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	re, err := regexp.Compile(pattern)
	if err != nil {
		return nil
	}

	result := make(map[string]float64)

	for term, postings := range idx.postings {
		if re.MatchString(term) {
			for _, p := range postings {
				result[p.DocID] += float64(p.TermFreq)
			}
		}
	}

	return result
}
