package vector

import (
	"math"
)

// DistanceFunc calculates the distance between two vectors
type DistanceFunc func(a, b []float32) float32

// GetDistanceFunc returns the distance function for the given metric
func GetDistanceFunc(metric DistanceMetric) (DistanceFunc, error) {
	switch metric {
	case DistanceCosine:
		return CosineDistance, nil
	case DistanceEuclidean:
		return EuclideanDistance, nil
	case DistanceDotProduct:
		return DotProductDistance, nil
	default:
		return nil, ErrInvalidDistanceMetric
	}
}

// CosineDistance calculates the cosine distance between two vectors
// Returns 1 - cosine_similarity, so lower values mean more similar
func CosineDistance(a, b []float32) float32 {
	if len(a) != len(b) {
		return float32(math.Inf(1))
	}

	var dotProduct, normA, normB float32
	for i := range a {
		dotProduct += a[i] * b[i]
		normA += a[i] * a[i]
		normB += b[i] * b[i]
	}

	if normA == 0 || normB == 0 {
		return float32(1.0) // Maximum distance for zero vectors
	}

	cosineSimilarity := dotProduct / (float32(math.Sqrt(float64(normA))) * float32(math.Sqrt(float64(normB))))
	
	// Convert similarity to distance (0 = identical, 2 = opposite)
	return 1 - cosineSimilarity
}

// CosineSimilarity calculates the cosine similarity between two vectors
// Returns a value between -1 and 1, where 1 means identical
func CosineSimilarity(a, b []float32) float32 {
	if len(a) != len(b) {
		return -1
	}

	var dotProduct, normA, normB float32
	for i := range a {
		dotProduct += a[i] * b[i]
		normA += a[i] * a[i]
		normB += b[i] * b[i]
	}

	if normA == 0 || normB == 0 {
		return 0
	}

	return dotProduct / (float32(math.Sqrt(float64(normA))) * float32(math.Sqrt(float64(normB))))
}

// EuclideanDistance calculates the Euclidean distance between two vectors
func EuclideanDistance(a, b []float32) float32 {
	if len(a) != len(b) {
		return float32(math.Inf(1))
	}

	var sum float32
	for i := range a {
		diff := a[i] - b[i]
		sum += diff * diff
	}

	return float32(math.Sqrt(float64(sum)))
}

// DotProductDistance calculates the negative dot product between two vectors
// We use negative dot product as distance so that higher dot product = lower distance = more similar
func DotProductDistance(a, b []float32) float32 {
	if len(a) != len(b) {
		return float32(math.Inf(1))
	}

	var dotProduct float32
	for i := range a {
		dotProduct += a[i] * b[i]
	}

	return -dotProduct
}

// DotProduct calculates the dot product between two vectors
func DotProduct(a, b []float32) float32 {
	if len(a) != len(b) {
		return 0
	}

	var dotProduct float32
	for i := range a {
		dotProduct += a[i] * b[i]
	}

	return dotProduct
}

// NormalizeVector normalizes a vector to unit length
func NormalizeVector(v []float32) []float32 {
	var norm float32
	for _, x := range v {
		norm += x * x
	}
	norm = float32(math.Sqrt(float64(norm)))

	if norm == 0 {
		return v
	}

	result := make([]float32, len(v))
	for i, x := range v {
		result[i] = x / norm
	}
	return result
}
