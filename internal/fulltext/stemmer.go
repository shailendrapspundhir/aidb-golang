package fulltext

import (
	"strings"

	"github.com/kljensen/snowball"
)

// Stemmer reduces words to their root form.
type Stemmer interface {
	// Stem returns the stemmed form of a token.
	Stem(token string) string
}

// EnglishStemmer uses the Snowball English stemmer.
type EnglishStemmer struct{}

// NewEnglishStemmer creates an English stemmer.
func NewEnglishStemmer() *EnglishStemmer {
	return &EnglishStemmer{}
}

// Stem returns the Snowball stem of the token.
func (s *EnglishStemmer) Stem(token string) string {
	if len(token) == 0 {
		return token
	}
	stemmed, err := snowball.Stem(token, "english", true)
	if err != nil {
		return token
	}
	return stemmed
}

// NoOpStemmer returns tokens unchanged.
type NoOpStemmer struct{}

// NewNoOpStemmer creates a no-op stemmer.
func NewNoOpStemmer() *NoOpStemmer {
	return &NoOpStemmer{}
}

// Stem returns the token unchanged.
func (s *NoOpStemmer) Stem(token string) string {
	return token
}

// StemFilter applies a stemmer to each token.
type StemFilter struct {
	stemmer Stemmer
}

// NewStemFilter creates a stem filter.
func NewStemFilter(stemmer Stemmer) *StemFilter {
	return &StemFilter{stemmer: stemmer}
}

// Filter stems all tokens.
func (f *StemFilter) Filter(tokens []string) []string {
	result := make([]string, len(tokens))
	for i, tok := range tokens {
		result[i] = f.stemmer.Stem(strings.ToLower(tok))
	}
	return result
}
