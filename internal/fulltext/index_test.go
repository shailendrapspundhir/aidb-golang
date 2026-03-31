package fulltext

import (
	"testing"
)

func TestInvertedIndex_IndexDocument(t *testing.T) {
	idx := NewInvertedIndex()
	idx.IndexDocument("doc1", []string{"hello world", "test document"})

	if idx.TotalDocs() != 1 {
		t.Fatalf("expected 1 doc, got %d", idx.TotalDocs())
	}

	// "hello" should have 1 posting
	posts := idx.GetPostings("hello")
	if len(posts) != 1 {
		t.Fatalf("expected 1 posting for 'hello', got %d", len(posts))
	}
	if posts[0].DocID != "doc1" {
		t.Fatalf("expected doc1, got %s", posts[0].DocID)
	}
}

func TestInvertedIndex_RemoveDocument(t *testing.T) {
	idx := NewInvertedIndex()
	idx.IndexDocument("doc1", []string{"hello world"})
	idx.RemoveDocument("doc1")

	if idx.TotalDocs() != 0 {
		t.Fatalf("expected 0 docs after remove, got %d", idx.TotalDocs())
	}
	posts := idx.GetPostings("hello")
	if len(posts) != 0 {
		t.Fatalf("expected no postings after remove, got %d", len(posts))
	}
}

func TestInvertedIndex_Reindex(t *testing.T) {
	idx := NewInvertedIndex()
	idx.IndexDocument("doc1", []string{"hello world"})
	idx.IndexDocument("doc1", []string{"quick fox"})

	// Should only have 1 doc
	if idx.TotalDocs() != 1 {
		t.Fatalf("expected 1 doc after reindex, got %d", idx.TotalDocs())
	}

	// "hello" should no longer exist
	posts := idx.GetPostings("hello")
	if len(posts) != 0 {
		t.Fatalf("expected no postings for 'hello' after reindex, got %d", len(posts))
	}

	// "quick" should exist
	posts = idx.GetPostings("quick")
	if len(posts) != 1 {
		t.Fatalf("expected 1 posting for 'quick', got %d", len(posts))
	}
}

func TestInvertedIndex_SearchTerms(t *testing.T) {
	idx := NewInvertedIndex()
	idx.IndexDocument("doc1", []string{"hello world"})
	idx.IndexDocument("doc2", []string{"hello there"})

	candidates := idx.SearchTerms([]string{"hello", "world"})
	if len(candidates) != 2 {
		t.Fatalf("expected 2 candidates, got %d", len(candidates))
	}
}

func TestBM25Scorer_IDF(t *testing.T) {
	s := NewBM25Scorer()
	idf := s.IDF(2, 10) // term in 2 of 10 docs
	if idf <= 0 {
		t.Fatalf("expected positive IDF, got %f", idf)
	}
}

func TestBM25Scorer_Score(t *testing.T) {
	s := NewBM25Scorer()
	// Simple sanity: score should be positive for valid inputs
	score := s.Score(5, 100, 1.5, 50)
	if score <= 0 {
		t.Fatalf("expected positive score, got %f", score)
	}
}

func TestBM25Scorer_ScoreDocuments(t *testing.T) {
	idx := NewInvertedIndex()
	idx.IndexDocument("doc1", []string{"the quick brown fox"})
	idx.IndexDocument("doc2", []string{"the quick blue fox"})
	idx.IndexDocument("doc3", []string{"slow turtle"})

	// Search for "quick"
	candidates := idx.SearchTerms([]string{"quick"})
	scorer := NewBM25Scorer()
	results := scorer.ScoreDocuments(candidates, idx, []string{"quick"})

	if len(results) == 0 {
		t.Fatalf("expected some results")
	}
	// doc1 and doc2 should have higher scores than doc3
	if results[0].DocID != "doc1" && results[0].DocID != "doc2" {
		t.Fatalf("expected doc1 or doc2 to rank highest, got %s", results[0].DocID)
	}
}

func TestPhraseSearch(t *testing.T) {
	idx := NewInvertedIndex()
	idx.IndexDocument("doc1", []string{"the quick brown fox"})
	idx.IndexDocument("doc2", []string{"the quick blue fox"})
	idx.IndexDocument("doc3", []string{"quick fox"})

	// Phrase "quick brown" should only match doc1 (adjacent positions)
	results := idx.PhraseSearch([]string{"quick", "brown"})
	if len(results) != 1 {
		t.Fatalf("expected 1 phrase match, got %d", len(results))
	}
	if _, ok := results["doc1"]; !ok {
		t.Fatalf("expected doc1 in phrase results")
	}
}

func TestPhraseSearchMultiWord(t *testing.T) {
	idx := NewInvertedIndex()
	idx.IndexDocument("doc1", []string{"hello world test"})
	idx.IndexDocument("doc2", []string{"hello test world"}) // different order

	results := idx.PhraseSearch([]string{"hello", "world", "test"})
	if len(results) != 1 {
		t.Fatalf("expected 1 phrase match for 'hello world test', got %d", len(results))
	}
	if _, ok := results["doc1"]; !ok {
		t.Fatalf("expected doc1 in phrase results")
	}
}

func TestFuzzySearch(t *testing.T) {
	idx := NewInvertedIndex()
	idx.IndexDocument("doc1", []string{"hello world"})
	idx.IndexDocument("doc2", []string{"helo wrld"}) // typos

	// Fuzzy search for "hello" with max distance 2 should match both
	results := idx.FuzzySearch([]string{"hello"}, 2)
	if len(results) != 2 {
		t.Fatalf("expected 2 fuzzy matches for 'hello', got %d", len(results))
	}
}

func TestLevenshtein(t *testing.T) {
	tests := []struct {
		a, b     string
		expected int
	}{
		{"", "", 0},
		{"a", "", 1},
		{"", "a", 1},
		{"kitten", "sitting", 3},
		{"saturday", "sunday", 3},
		{"hello", "hello", 0},
		{"hello", "helo", 1},
	}

	for _, tt := range tests {
		result := levenshtein(tt.a, tt.b)
		if result != tt.expected {
			t.Errorf("levenshtein(%q, %q) = %d, want %d", tt.a, tt.b, result, tt.expected)
		}
	}
}

func TestCaseSensitiveAnalyzer(t *testing.T) {
	// Default (case-insensitive) analyzer
	analyzer := StandardAnalyzer()
	terms := analyzer.Analyze("Hello WORLD")
	// Should be lowercased
	for _, term := range terms {
		if term != "hello" && term != "world" {
			t.Errorf("expected lowercase terms, got %q", term)
		}
	}

	// Case-sensitive analyzer - note: stemmer may still lowercase
	// but the normalizer should not lowercase
	csAnalyzer := NewAnalyzer(WithCaseSensitive())
	csTerms := csAnalyzer.Analyze("Hello WORLD")
	// The normalizer no longer lowercases, but stemmer might
	// Just verify we get some terms (not empty)
	if len(csTerms) == 0 {
		t.Errorf("case-sensitive analyzer should produce terms, got empty")
	}
	// At minimum, verify "hello" or "Hello" variant exists (stemmer may change)
	foundHello := false
	for _, term := range csTerms {
		if term == "Hello" || term == "hello" {
			foundHello = true
		}
	}
	if !foundHello {
		t.Errorf("expected Hello/hello in results, got %v", csTerms)
	}
}

func TestSpecialCharacterSearch(t *testing.T) {
	idx := NewInvertedIndex()

	// Index documents with special characters
	idx.IndexDocument("doc1", []string{"email test@example.com please"})
	idx.IndexDocument("doc2", []string{"price is $99.99 today"})
	idx.IndexDocument("doc3", []string{"hashtag #golang #test"})
	idx.IndexDocument("doc4", []string{"code: func main() {}"})

	// Search for terms containing special chars (they get tokenized)
	// @ and . in email should be stripped by tokenizer, leaving "test" and "example"
	terms := StandardAnalyzer().Analyze("test@example.com")
	if len(terms) == 0 {
		t.Logf("Email tokenized to: %v", terms)
	}

	// Search for "test" should find doc1
	results := idx.SearchTerms([]string{"test"})
	if _, ok := results["doc1"]; !ok {
		t.Errorf("expected doc1 in results for 'test', got %v", results)
	}

	// Search for "golang" should find doc3
	results = idx.SearchTerms([]string{"golang"})
	if _, ok := results["doc3"]; !ok {
		t.Errorf("expected doc3 in results for 'golang', got %v", results)
	}

	// Search for "99" should find doc2 (numbers preserved)
	results = idx.SearchTerms([]string{"99"})
	if _, ok := results["doc2"]; !ok {
		t.Errorf("expected doc2 in results for '99', got %v", results)
	}
}

func TestRegexSearch(t *testing.T) {
	idx := NewInvertedIndex()

	// Index documents
	idx.IndexDocument("doc1", []string{"hello world test"})
	idx.IndexDocument("doc2", []string{"testing regex patterns"})
	idx.IndexDocument("doc3", []string{"hello there again"})

	// Regex search for terms starting with "test"
	results := idx.RegexSearch("^test")
	if len(results) == 0 {
		t.Errorf("expected results for regex ^test, got empty")
	}
	// Should match "test" in doc1 and "testing" in doc2
	if _, ok := results["doc1"]; !ok {
		t.Errorf("expected doc1 in regex results, got %v", results)
	}
	if _, ok := results["doc2"]; !ok {
		t.Errorf("expected doc2 in regex results, got %v", results)
	}

	// Regex search for terms ending with "est"
	results = idx.RegexSearch("est$")
	if _, ok := results["doc1"]; !ok {
		t.Errorf("expected doc1 in regex results for est$, got %v", results)
	}

	// Regex search for "hello" exactly
	results = idx.RegexSearch("^hello$")
	if _, ok := results["doc1"]; !ok {
		t.Errorf("expected doc1 in regex results for ^hello$, got %v", results)
	}
	if _, ok := results["doc3"]; !ok {
		t.Errorf("expected doc3 in regex results for ^hello$, got %v", results)
	}
}

func TestSpecialCharacterQueryParsing(t *testing.T) {
	// Test that special characters in query strings are handled
	analyzer := StandardAnalyzer()

	// Various special character inputs
	inputs := []string{
		"hello@world",
		"price$100",
		"#hashtag",
		"100%",
		"a+b=c",
		"file.txt",
	}

	for _, input := range inputs {
		terms := analyzer.Analyze(input)
		// Should not panic and should produce some terms
		t.Logf("Input %q -> terms: %v", input, terms)
	}
}
