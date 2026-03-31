package fulltext

// Analyzer processes text through the full pipeline: tokenize → normalize → filter → stem.
type Analyzer struct {
	tokenizer Tokenizer
	normalizer Normalizer
	filter    Filter
	stemmer   Stemmer
}

// AnalyzerOption configures an Analyzer.
type AnalyzerOption func(*Analyzer)

// WithTokenizer sets a custom tokenizer.
func WithTokenizer(t Tokenizer) AnalyzerOption {
	return func(a *Analyzer) { a.tokenizer = t }
}

// WithNormalizer sets a custom normalizer.
func WithNormalizer(n Normalizer) AnalyzerOption {
	return func(a *Analyzer) { a.normalizer = n }
}

// WithFilter sets a custom filter chain.
func WithFilter(f Filter) AnalyzerOption {
	return func(a *Analyzer) { a.filter = f }
}

// WithStemmer sets a custom stemmer.
func WithStemmer(s Stemmer) AnalyzerOption {
	return func(a *Analyzer) { a.stemmer = s }
}

// WithCaseSensitive creates an analyzer that preserves case (no lowercase normalization).
// Note: The default is case-insensitive (lowercasing). Use this for case-sensitive indexing/search.
func WithCaseSensitive() AnalyzerOption {
	return func(a *Analyzer) {
		// Replace the normalizer chain: Unicode only, no lowercase
		a.normalizer = NewUnicodeNormalizer(nil)
	}
}

// NewAnalyzer creates a new Analyzer with standard defaults.
// Default pipeline: UnicodeTokenizer → UnicodeNormalizer → Lowercase → EnglishStopwords → MinLength(2) → EnglishStemmer
func NewAnalyzer(opts ...AnalyzerOption) *Analyzer {
	a := &Analyzer{
		tokenizer:  NewUnicodeTokenizer(),
		normalizer: NewUnicodeNormalizer(NewLowercaseNormalizer()),
		filter: NewChainedFilter(
			NewEnglishStopwordFilter(),
			NewMinLengthFilter(2),
		),
		stemmer: NewEnglishStemmer(),
	}
	for _, opt := range opts {
		opt(a)
	}
	return a
}

// Analyze processes text into normalized, stemmed terms.
func (a *Analyzer) Analyze(text string) []string {
	// 1. Tokenize
	tokens := a.tokenizer.Tokenize(text)
	if len(tokens) == 0 {
		return tokens
	}

	// 2. Normalize each token
	for i, tok := range tokens {
		tokens[i] = a.normalizer.Normalize(tok)
	}

	// 3. Filter
	tokens = a.filter.Filter(tokens)

	// 4. Stem
	if a.stemmer != nil {
		for i, tok := range tokens {
			tokens[i] = a.stemmer.Stem(tok)
		}
	}

	return tokens
}

// StandardAnalyzer is a convenience that returns a standard English analyzer.
func StandardAnalyzer() *Analyzer {
	return NewAnalyzer()
}
