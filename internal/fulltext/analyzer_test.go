package fulltext

import (
	"testing"
)

func TestWhitespaceTokenizer(t *testing.T) {
	tok := NewWhitespaceTokenizer()
	tokens := tok.Tokenize("hello world  test")
	if len(tokens) != 3 {
		t.Fatalf("expected 3 tokens, got %d: %v", len(tokens), tokens)
	}
	if tokens[0] != "hello" || tokens[1] != "world" || tokens[2] != "test" {
		t.Fatalf("unexpected tokens: %v", tokens)
	}
}

func TestUnicodeTokenizer(t *testing.T) {
	tok := NewUnicodeTokenizer()
	tokens := tok.Tokenize("Hello, World! Testing 123.")
	if len(tokens) != 4 {
		t.Fatalf("expected 4 tokens, got %d: %v", len(tokens), tokens)
	}
	// Should be: Hello, World, Testing, 123
}

func TestLowercaseNormalizer(t *testing.T) {
	n := NewLowercaseNormalizer()
	if n.Normalize("HELLO") != "hello" {
		t.Fatalf("expected 'hello', got '%s'", n.Normalize("HELLO"))
	}
}

func TestUnicodeNormalizer(t *testing.T) {
	n := NewUnicodeNormalizer(NewLowercaseNormalizer())
	// NFKC: ﬁ -> fi
	result := n.Normalize("ﬁle")
	if result != "file" {
		t.Fatalf("expected 'file', got '%s'", result)
	}
}

func TestEnglishStopwordFilter(t *testing.T) {
	f := NewEnglishStopwordFilter()
	tokens := []string{"hello", "the", "world", "a", "test"}
	filtered := f.Filter(tokens)
	// Should remove "the" and "a"
	if len(filtered) != 3 {
		t.Fatalf("expected 3 tokens after filter, got %d: %v", len(filtered), filtered)
	}
}

func TestMinLengthFilter(t *testing.T) {
	f := NewMinLengthFilter(3)
	tokens := []string{"hi", "hello", "a", "test"}
	filtered := f.Filter(tokens)
	if len(filtered) != 2 {
		t.Fatalf("expected 2 tokens, got %d: %v", len(filtered), filtered)
	}
}

func TestEnglishStemmer(t *testing.T) {
	s := NewEnglishStemmer()
	// "running" should stem to "run"
	stemmed := s.Stem("running")
	if stemmed != "run" {
		t.Fatalf("expected 'run', got '%s'", stemmed)
	}
}

func TestAnalyzerPipeline(t *testing.T) {
	a := StandardAnalyzer()
	terms := a.Analyze("The quick brown foxes are running fast!")
	// After pipeline: quick, brown, fox, run, fast (stopwords removed, stemmed)
	if len(terms) == 0 {
		t.Fatalf("expected some terms, got none")
	}
	// Check that stopwords are removed
	for _, term := range terms {
		if term == "the" || term == "are" {
			t.Fatalf("stopwords should be filtered, found '%s'", term)
		}
	}
}

func TestAnalyzerEmptyInput(t *testing.T) {
	a := StandardAnalyzer()
	terms := a.Analyze("")
	if len(terms) != 0 {
		t.Fatalf("expected no terms for empty input, got %v", terms)
	}
}
