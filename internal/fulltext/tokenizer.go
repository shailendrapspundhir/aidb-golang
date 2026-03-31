package fulltext

import (
	"unicode"
)

// Tokenizer splits text into tokens/terms.
type Tokenizer interface {
	// Tokenize splits the input text into a slice of tokens.
	Tokenize(text string) []string
}

// WhitespaceTokenizer splits on whitespace characters.
type WhitespaceTokenizer struct{}

// NewWhitespaceTokenizer creates a new whitespace tokenizer.
func NewWhitespaceTokenizer() *WhitespaceTokenizer {
	return &WhitespaceTokenizer{}
}

// Tokenize splits text on whitespace.
func (t *WhitespaceTokenizer) Tokenize(text string) []string {
	tokens := make([]string, 0)
	start := 0
	inToken := false

	for i, r := range text {
		if unicode.IsSpace(r) {
			if inToken {
				tokens = append(tokens, text[start:i])
				inToken = false
			}
		} else {
			if !inToken {
				start = i
				inToken = true
			}
		}
	}
	// Handle last token
	if inToken {
		tokens = append(tokens, text[start:])
	}

	return tokens
}

// UnicodeTokenizer splits on non-letter/non-digit characters (unicode-aware).
type UnicodeTokenizer struct{}

// NewUnicodeTokenizer creates a new unicode-aware tokenizer.
func NewUnicodeTokenizer() *UnicodeTokenizer {
	return &UnicodeTokenizer{}
}

// Tokenize splits text into sequences of letters/digits (unicode-aware).
func (t *UnicodeTokenizer) Tokenize(text string) []string {
	tokens := make([]string, 0)
	start := -1

	for i, r := range text {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			if start == -1 {
				start = i
			}
		} else {
			if start != -1 {
				tokens = append(tokens, text[start:i])
				start = -1
			}
		}
	}
	// Handle last token
	if start != -1 {
		tokens = append(tokens, text[start:])
	}

	return tokens
}
