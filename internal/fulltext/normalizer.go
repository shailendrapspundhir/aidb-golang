package fulltext

import (
	"strings"
	"unicode"

	"golang.org/x/text/unicode/norm"
)

// Normalizer transforms tokens into normalized form.
type Normalizer interface {
	// Normalize transforms a single token.
	Normalize(token string) string
}

// LowercaseNormalizer converts tokens to lowercase.
type LowercaseNormalizer struct{}

// NewLowercaseNormalizer creates a lowercase normalizer.
func NewLowercaseNormalizer() *LowercaseNormalizer {
	return &LowercaseNormalizer{}
}

// Normalize converts token to lowercase.
func (n *LowercaseNormalizer) Normalize(token string) string {
	return strings.ToLower(token)
}

// UnicodeNormalizer performs Unicode normalization (NFKC).
type UnicodeNormalizer struct {
	inner Normalizer
}

// NewUnicodeNormalizer creates a Unicode NFKC normalizer with optional inner normalizer.
func NewUnicodeNormalizer(inner Normalizer) *UnicodeNormalizer {
	return &UnicodeNormalizer{inner: inner}
}

// Normalize applies NFKC normalization and optionally delegates to inner normalizer.
func (n *UnicodeNormalizer) Normalize(token string) string {
	// NFKC normalizes compatibility characters (e.g., ﬁ -> fi, ① -> 1)
	normalized := norm.NFKC.String(token)
	if n.inner != nil {
		return n.inner.Normalize(normalized)
	}
	return normalized
}

// ASCIIFoldNormalizer removes diacritics (e.g., é -> e).
type ASCIIFoldNormalizer struct {
	inner Normalizer
}

// NewASCIIFoldNormalizer creates an ASCII folding normalizer.
func NewASCIIFoldNormalizer(inner Normalizer) *ASCIIFoldNormalizer {
	return &ASCIIFoldNormalizer{inner: inner}
}

// Normalize removes diacritics from token.
func (n *ASCIIFoldNormalizer) Normalize(token string) string {
	var b strings.Builder
	b.Grow(len(token))

	for _, r := range token {
		// Simple ASCII fold: strip combining marks
		if unicode.IsMark(r) {
			continue
		}
		// Map common accented chars
		r = foldRune(r)
		b.WriteRune(r)
	}

	result := b.String()
	if n.inner != nil {
		return n.inner.Normalize(result)
	}
	return result
}

// foldRune maps common accented characters to ASCII.
func foldRune(r rune) rune {
	switch r {
	case 'á', 'à', 'â', 'ä', 'ã', 'å':
		return 'a'
	case 'é', 'è', 'ê', 'ë':
		return 'e'
	case 'í', 'ì', 'î', 'ï':
		return 'i'
	case 'ó', 'ò', 'ô', 'ö', 'õ':
		return 'o'
	case 'ú', 'ù', 'û', 'ü':
		return 'u'
	case 'ý', 'ÿ':
		return 'y'
	case 'ñ':
		return 'n'
	case 'ç':
		return 'c'
	case 'ß':
		return 's'
	}
	return r
}
