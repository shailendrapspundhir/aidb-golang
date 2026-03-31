package fulltext

import (
	"strings"
)

// Filter removes or transforms tokens based on criteria.
type Filter interface {
	// Filter takes a slice of tokens and returns filtered tokens.
	Filter(tokens []string) []string
}

// StopwordFilter removes common stopwords.
type StopwordFilter struct {
	stopwords map[string]struct{}
}

// NewStopwordFilter creates a stopword filter with the given stopwords.
func NewStopwordFilter(stopwords []string) *StopwordFilter {
	sw := make(map[string]struct{}, len(stopwords))
	for _, w := range stopwords {
		sw[strings.ToLower(w)] = struct{}{}
	}
	return &StopwordFilter{stopwords: sw}
}

// Default English stopwords.
var englishStopwords = []string{
	"a", "an", "the", "and", "or", "but", "in", "on", "at", "to", "for", "of", "with",
	"by", "from", "as", "is", "was", "are", "were", "been", "be", "have", "has", "had",
	"do", "does", "did", "will", "would", "could", "should", "may", "might", "must",
	"shall", "can", "need", "dare", "ought", "used", "it's", "its", "this", "that",
	"these", "those", "i", "you", "he", "she", "it", "we", "they", "what", "which",
	"who", "whom", "whose", "where", "when", "why", "how", "all", "each", "every",
	"both", "few", "more", "most", "other", "some", "such", "no", "nor", "not",
	"only", "own", "same", "so", "than", "too", "very", "just", "also", "now",
	"here", "there", "then", "once", "if", "because", "until", "while", "about",
	"against", "between", "into", "through", "during", "before", "after", "above",
	"below", "under", "again", "further", "over", "under", "up", "down", "out",
	"over", "under", "again", "further", "then", "once", "here", "there", "when",
	"where", "why", "how", "all", "each", "few", "more", "most", "other", "some",
	"such", "no", "nor", "not", "only", "own", "same", "so", "than", "too", "very",
	"s", "t", "can", "will", "just", "don", "now", "i", "me", "my", "myself", "we",
	"our", "ours", "ourselves", "you", "your", "yours", "yourself", "yourselves",
	"he", "him", "his", "himself", "she", "her", "hers", "herself", "it", "its",
	"itself", "they", "them", "their", "theirs", "themselves", "what", "which", "who",
	"whom", "this", "that", "these", "those", "am", "is", "are", "was", "were", "be",
	"been", "being", "have", "has", "had", "having", "do", "does", "did", "doing",
	"would", "should", "could", "ought", "might", "must", "shall", "can", "need",
	"dare", "used", "a", "an", "the", "and", "but", "if", "or", "because", "as",
	"until", "while", "of", "at", "by", "for", "with", "about", "against", "between",
	"into", "through", "during", "before", "after", "above", "below", "to", "from",
	"up", "down", "in", "out", "on", "off", "over", "under", "again", "further",
	"then", "once", "here", "there", "where", "why", "how", "all", "each", "few",
	"more", "most", "other", "some", "such", "no", "nor", "not", "only", "own",
	"same", "so", "than", "too", "very", "s", "t", "can", "will", "just", "don",
	"now", "i", "me", "my", "myself", "we", "our", "ours", "ourselves", "you",
	"your", "yours", "yourself", "yourselves", "he", "him", "his", "himself",
	"she", "her", "hers", "herself", "it", "its", "itself", "they", "them",
	"their", "theirs", "themselves", "what", "which", "who", "whom", "this",
	"that", "these", "those", "am", "is", "are", "was", "were", "be", "been",
	"being", "have", "has", "had", "having", "do", "does", "did", "doing",
	"would", "should", "could", "ought", "might", "must", "shall", "can", "need",
	"dare", "used", "a", "an", "the", "and", "but", "if", "or", "because", "as",
	"until", "while", "of", "at", "by", "for", "with", "about", "against", "between",
	"into", "through", "during", "before", "after", "above", "below", "to", "from",
	"up", "down", "in", "out", "on", "off", "over", "under", "again", "further",
	"then", "once", "here", "there", "where", "why", "how", "all", "each", "few",
	"more", "most", "other", "some", "such", "no", "nor", "not", "only", "own",
	"same", "so", "than", "too", "very", "s", "t", "can", "will", "just", "don",
	"now",
}

// NewEnglishStopwordFilter creates a filter with default English stopwords.
func NewEnglishStopwordFilter() *StopwordFilter {
	return NewStopwordFilter(englishStopwords)
}

// Filter removes stopwords from tokens.
func (f *StopwordFilter) Filter(tokens []string) []string {
	result := make([]string, 0, len(tokens))
	for _, tok := range tokens {
		if _, stop := f.stopwords[strings.ToLower(tok)]; !stop {
			result = append(result, tok)
		}
	}
	return result
}

// MinLengthFilter removes tokens shorter than minLen.
type MinLengthFilter struct {
	minLen int
}

// NewMinLengthFilter creates a min-length filter.
func NewMinLengthFilter(minLen int) *MinLengthFilter {
	return &MinLengthFilter{minLen: minLen}
}

// Filter removes short tokens.
func (f *MinLengthFilter) Filter(tokens []string) []string {
	result := make([]string, 0, len(tokens))
	for _, tok := range tokens {
		if len(tok) >= f.minLen {
			result = append(result, tok)
		}
	}
	return result
}

// ChainedFilter applies multiple filters in sequence.
type ChainedFilter struct {
	filters []Filter
}

// NewChainedFilter creates a filter chain.
func NewChainedFilter(filters ...Filter) *ChainedFilter {
	return &ChainedFilter{filters: filters}
}

// Filter applies all filters in order.
func (c *ChainedFilter) Filter(tokens []string) []string {
	for _, f := range c.filters {
		tokens = f.Filter(tokens)
	}
	return tokens
}
