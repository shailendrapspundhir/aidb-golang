package collection

import (
	"fmt"
	"math"
	"sort"
	"strings"
)

// AggregationPipeline represents a MongoDB-like aggregation pipeline
type AggregationPipeline struct {
	stages []PipelineStage
}

// PipelineStage represents a single stage in the aggregation pipeline
type PipelineStage struct {
	// Stage operators
	Match       *MatchStage       `json:"$match,omitempty"`
	Group       *GroupStage       `json:"$group,omitempty"`
	Sort        *SortStage        `json:"$sort,omitempty"`
	Project     *ProjectStage     `json:"$project,omitempty"`
	Limit       *int              `json:"$limit,omitempty"`
	Skip        *int              `json:"$skip,omitempty"`
	AddFields   map[string]interface{} `json:"$addFields,omitempty"`
	Unset       interface{}       `json:"$unset,omitempty"`
	Lookup      *LookupStage      `json:"$lookup,omitempty"`
	Unwind      *UnwindStage      `json:"$unwind,omitempty"`
	Count       string            `json:"$count,omitempty"`
	Facet       map[string][]PipelineStage `json:"$facet,omitempty"`
	Bucket      *BucketStage      `json:"$bucket,omitempty"`
	SortByCount *SortByCountStage `json:"$sortByCount,omitempty"`
	ReplaceRoot interface{}       `json:"$replaceRoot,omitempty"`
	Set         map[string]interface{} `json:"$set,omitempty"`
}

// MatchStage filters documents
type MatchStage struct {
	// Simple field matches
	Fields map[string]interface{} `json:"-"`
	
	// Comparison operators
	Eq    map[string]interface{} `json:"$eq,omitempty"`
	Ne    map[string]interface{} `json:"$ne,omitempty"`
	Gt    map[string]interface{} `json:"$gt,omitempty"`
	Gte   map[string]interface{} `json:"$gte,omitempty"`
	Lt    map[string]interface{} `json:"$lt,omitempty"`
	Lte   map[string]interface{} `json:"$lte,omitempty"`
	In    map[string][]interface{} `json:"$in,omitempty"`
	Nin   map[string][]interface{} `json:"$nin,omitempty"`
	
	// Logical operators
	And []map[string]interface{} `json:"$and,omitempty"`
	Or  []map[string]interface{} `json:"$or,omitempty"`
	Not map[string]interface{}   `json:"$not,omitempty"`
	Nor []map[string]interface{} `json:"$nor,omitempty"`
	
	// Element operators
	Exists    map[string]bool `json:"$exists,omitempty"`
	Type      map[string]string `json:"$type,omitempty"`
	
	// Evaluation operators
	Regex   map[string]string `json:"$regex,omitempty"`
	Expr    interface{}       `json:"$expr,omitempty"`
	Mod     map[string][]int  `json:"$mod,omitempty"`
	
	// Array operators
	All        map[string][]interface{} `json:"$all,omitempty"`
	ElemMatch  map[string]interface{}   `json:"$elemMatch,omitempty"`
	Size       map[string]int           `json:"$size,omitempty"`
}

// GroupStage groups documents
type GroupStage struct {
	ID interface{} `json:"_id,omitempty"`
	
	// Accumulators
	Sum       map[string]interface{} `json:"$sum,omitempty"`
	Avg       map[string]interface{} `json:"$avg,omitempty"`
	Min       map[string]interface{} `json:"$min,omitempty"`
	Max       map[string]interface{} `json:"$max,omitempty"`
	Count     map[string]interface{} `json:"$count,omitempty"`
	First     map[string]interface{} `json:"$first,omitempty"`
	Last      map[string]interface{} `json:"$last,omitempty"`
	Push      map[string]interface{} `json:"$push,omitempty"`
	AddToSet  map[string]interface{} `json:"$addToSet,omitempty"`
	StdDevPop map[string]interface{} `json:"$stdDevPop,omitempty"`
	StdDevSamp map[string]interface{} `json:"$stdDevSamp,omitempty"`
}

// SortStage sorts documents
type SortStage struct {
	Fields map[string]int `json:"-"` // 1 for ascending, -1 for descending
}

// ProjectStage reshapes documents
type ProjectStage struct {
	Fields    map[string]interface{} `json:"-"`
	Included  []string               `json:"-"`
	Excluded  []string               `json:"-"`
	Computed  map[string]interface{} `json:"-"`
}

// LookupStage performs a left outer join
type LookupStage struct {
	From         string `json:"from"`
	LocalField   string `json:"localField"`
	ForeignField string `json:"foreignField"`
	As           string `json:"as"`
	
	// For uncorrelated subqueries
	Let         map[string]interface{} `json:"let,omitempty"`
	Pipeline    []PipelineStage        `json:"pipeline,omitempty"`
}

// UnwindStage deconstructs an array field
type UnwindStage struct {
	Path                       string `json:"path"`
	IncludeArrayIndex          string `json:"includeArrayIndex,omitempty"`
	PreserveNullAndEmptyArrays bool   `json:"preserveNullAndEmptyArrays,omitempty"`
}

// BucketStage categorizes documents into groups
type BucketStage struct {
	GroupBy       interface{} `json:"groupBy"`
	Boundaries    []interface{} `json:"boundaries"`
	Default       interface{} `json:"default,omitempty"`
	Output        map[string]interface{} `json:"output,omitempty"`
}

// SortByCountStage groups and sorts by count
type SortByCountStage struct {
	Field string `json:"-"`
}

// NewAggregationPipeline creates a new aggregation pipeline
func NewAggregationPipeline(stages ...PipelineStage) *AggregationPipeline {
	return &AggregationPipeline{stages: stages}
}

// AddStage adds a stage to the pipeline
func (p *AggregationPipeline) AddStage(stage PipelineStage) {
	p.stages = append(p.stages, stage)
}

// Execute runs the aggregation pipeline on a collection using streaming cursor
func (c *Collection) ExecuteAggregation(pipeline *AggregationPipeline) ([]map[string]interface{}, error) {
	// Streaming cursor-based iteration to avoid loading all documents into memory
	cursor, err := c.storage.Cursor()
	if err != nil {
		return nil, fmt.Errorf("failed to create cursor: %w", err)
	}
	defer cursor.Close()

	// Accumulate results; streaming stages applied per-doc
	results := make([]map[string]interface{}, 0)

	for cursor.Next() {
		doc := cursor.Current()
		if doc == nil {
			continue
		}

		// Convert to map with _id
		docMap := make(map[string]interface{}, len(doc.Data)+1)
		for k, v := range doc.Data {
			docMap[k] = v
		}
		docMap["_id"] = doc.ID

		// Apply $match early (streaming filter) - main memory saver for selective queries
		skipDoc := false
		for _, stage := range pipeline.stages {
			if stage.Match != nil {
				if !matchesConditions(docMap, stage.Match) {
					skipDoc = true
					break
				}
			} else if stage.Lookup != nil || stage.Group != nil || stage.Sort != nil ||
				stage.Facet != nil || stage.Bucket != nil || stage.SortByCount != nil ||
				stage.Skip != nil || stage.Limit != nil || stage.Count != "" ||
				stage.Project != nil || stage.AddFields != nil || stage.Set != nil ||
				stage.Unset != nil || stage.ReplaceRoot != nil || stage.Unwind != nil {
				// Non-match stage; stop per-doc processing, accumulate for full pipeline
				break
			}
		}

		if skipDoc {
			continue
		}
		results = append(results, docMap)
	}

	if err := cursor.Err(); err != nil {
		return nil, fmt.Errorf("cursor error: %w", err)
	}

	// Now process remaining stages on accumulated results
	// We need to re-run the pipeline, but streaming stages on already-transformed docs are safe
	// (match on already-matched data is redundant but correct; project on projected data re-applies)
	for _, stage := range pipeline.stages {
		results, err = c.executeStage(results, stage)
		if err != nil {
			return nil, err
		}
	}

	return results, nil
}

// executeProjectSingle applies $project to a single document
func (c *Collection) executeProjectSingle(doc map[string]interface{}, project *ProjectStage) (map[string]interface{}, error) {
	result := make(map[string]interface{})

	// Handle explicit inclusions/exclusions
	for field, value := range project.Fields {
		switch v := value.(type) {
		case int:
			if v == 1 {
				if val, exists := doc[field]; exists {
					result[field] = val
				}
			}
			// v == 0 means exclude
		case bool:
			if v {
				if val, exists := doc[field]; exists {
					result[field] = val
				}
			}
			// v == false means exclude
		default:
			// Computed field or expression
			result[field] = value
		}
	}

	// If project is empty (or only has _id handling), include all fields by default
	if len(project.Fields) == 0 {
		for k, v := range doc {
			result[k] = v
		}
	}

	// Handle _id specially
	if idVal, exists := project.Fields["_id"]; exists {
		if v, ok := idVal.(int); ok && v == 0 {
			delete(result, "_id")
		}
	}

	return result, nil
}

// executeAddFieldsSingle applies $addFields or $set to a single document
func (c *Collection) executeAddFieldsSingle(doc map[string]interface{}, addFields map[string]interface{}) (map[string]interface{}, error) {
	result := make(map[string]interface{}, len(doc)+len(addFields))
	for k, v := range doc {
		result[k] = v
	}
	for k, v := range addFields {
		result[k] = v
	}
	return result, nil
}

// executeUnsetSingle removes fields from a document
func (c *Collection) executeUnsetSingle(doc map[string]interface{}, unset interface{}) map[string]interface{} {
	result := make(map[string]interface{}, len(doc))
	for k, v := range doc {
		result[k] = v
	}

	switch u := unset.(type) {
	case string:
		delete(result, u)
	case []interface{}:
		for _, f := range u {
			if s, ok := f.(string); ok {
				delete(result, s)
			}
		}
	case []string:
		for _, f := range u {
			delete(result, f)
		}
	}
	return result
}

// executeReplaceRootSingle applies $replaceRoot to a single document
func (c *Collection) executeReplaceRootSingle(doc map[string]interface{}, replaceRoot interface{}) (map[string]interface{}, error) {
	switch rr := replaceRoot.(type) {
	case map[string]interface{}:
		if newRoot, ok := rr["newRoot"]; ok {
			switch nr := newRoot.(type) {
			case string:
				// newRoot is a field path like "$embedded"
				if val, exists := doc[nr]; exists {
					if nested, ok := val.(map[string]interface{}); ok {
						nested["_id"] = doc["_id"]
						return nested, nil
					}
				}
			case map[string]interface{}:
				return nr, nil
			}
		}
	}
	// Fallback: return original
	return doc, nil
}

// executeStage executes a single pipeline stage
func (c *Collection) executeStage(docs []map[string]interface{}, stage PipelineStage) ([]map[string]interface{}, error) {
	switch {
	case stage.Match != nil:
		return c.executeMatch(docs, stage.Match)
	case stage.Group != nil:
		return c.executeGroup(docs, stage.Group)
	case stage.Sort != nil:
		return c.executeSort(docs, stage.Sort)
	case stage.Project != nil:
		return c.executeProject(docs, stage.Project)
	case stage.Limit != nil:
		return c.executeLimit(docs, *stage.Limit)
	case stage.Skip != nil:
		return c.executeSkip(docs, *stage.Skip)
	case stage.AddFields != nil:
		return c.executeAddFields(docs, stage.AddFields)
	case stage.Unset != nil:
		return c.executeUnset(docs, stage.Unset)
	case stage.Lookup != nil:
		return c.executeLookup(docs, stage.Lookup)
	case stage.Unwind != nil:
		return c.executeUnwind(docs, stage.Unwind)
	case stage.Count != "":
		return c.executeCount(docs, stage.Count)
	case stage.Facet != nil:
		return c.executeFacet(docs, stage.Facet)
	case stage.Bucket != nil:
		return c.executeBucket(docs, stage.Bucket)
	case stage.SortByCount != nil:
		return c.executeSortByCount(docs, stage.SortByCount)
	case stage.ReplaceRoot != nil:
		return c.executeReplaceRoot(docs, stage.ReplaceRoot)
	case stage.Set != nil:
		return c.executeAddFields(docs, stage.Set) // $set is alias for $addFields
	default:
		return docs, nil
	}
}

// executeMatch filters documents based on conditions
func (c *Collection) executeMatch(docs []map[string]interface{}, match *MatchStage) ([]map[string]interface{}, error) {
	var results []map[string]interface{}

	for _, doc := range docs {
		if matchesConditions(doc, match) {
			results = append(results, doc)
		}
	}

	return results, nil
}

// matchesConditions checks if a document matches all match conditions
func matchesConditions(doc map[string]interface{}, match *MatchStage) bool {
	// Check simple field matches
	for field, value := range match.Fields {
		if !matchField(doc, field, value) {
			return false
		}
	}

	// Check $and conditions
	if len(match.And) > 0 {
		for _, cond := range match.And {
			if !matchSimpleConditions(doc, cond) {
				return false
			}
		}
	}

	// Check $or conditions
	if len(match.Or) > 0 {
		matched := false
		for _, cond := range match.Or {
			if matchSimpleConditions(doc, cond) {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}

	// Check $nor conditions
	if len(match.Nor) > 0 {
		for _, cond := range match.Nor {
			if matchSimpleConditions(doc, cond) {
				return false
			}
		}
	}

	// Check comparison operators
	for field, value := range match.Eq {
		if getFieldValue(doc, field) != value {
			return false
		}
	}

	for field, value := range match.Ne {
		if getFieldValue(doc, field) == value {
			return false
		}
	}

	for field, value := range match.Gt {
		if !compareGreater(doc, field, value) {
			return false
		}
	}

	for field, value := range match.Gte {
		if !compareGreaterOrEqual(doc, field, value) {
			return false
		}
	}

	for field, value := range match.Lt {
		if !compareLess(doc, field, value) {
			return false
		}
	}

	for field, value := range match.Lte {
		if !compareLessOrEqual(doc, field, value) {
			return false
		}
	}

	for field, values := range match.In {
		if !isInArray(doc, field, values) {
			return false
		}
	}

	for field, values := range match.Nin {
		if isInArray(doc, field, values) {
			return false
		}
	}

	// Check $exists
	for field, exists := range match.Exists {
		_, found := getNestedField(doc, field)
		if found != exists {
			return false
		}
	}

	// Check $regex
	for field, pattern := range match.Regex {
		value := getFieldValue(doc, field)
		if str, ok := value.(string); ok {
			if !strings.Contains(str, pattern) {
				return false
			}
		} else {
			return false
		}
	}

	return true
}

// matchSimpleConditions matches simple field: value conditions
func matchSimpleConditions(doc map[string]interface{}, conds map[string]interface{}) bool {
	for field, value := range conds {
		if !matchField(doc, field, value) {
			return false
		}
	}
	return true
}

// matchField matches a single field condition
func matchField(doc map[string]interface{}, field string, value interface{}) bool {
	docValue := getFieldValue(doc, field)
	
	// Handle comparison operators in value
	if m, ok := value.(map[string]interface{}); ok {
		for op, v := range m {
			switch op {
			case "$eq":
				if docValue != v {
					return false
				}
			case "$ne":
				if docValue == v {
					return false
				}
			case "$gt":
				if !compareValues(docValue, v, ">") {
					return false
				}
			case "$gte":
				if !compareValues(docValue, v, ">=") {
					return false
				}
			case "$lt":
				if !compareValues(docValue, v, "<") {
					return false
				}
			case "$lte":
				if !compareValues(docValue, v, "<=") {
					return false
				}
			case "$in":
				if arr, ok := v.([]interface{}); ok {
					if !containsValue(arr, docValue) {
						return false
					}
				}
			case "$nin":
				if arr, ok := v.([]interface{}); ok {
					if containsValue(arr, docValue) {
						return false
					}
				}
			case "$exists":
				_, found := getNestedField(doc, field)
				if exists, ok := v.(bool); ok {
					if found != exists {
						return false
					}
				}
			case "$regex":
				if str, ok := docValue.(string); ok {
					if pattern, ok := v.(string); ok {
						if !strings.Contains(str, pattern) {
							return false
						}
					}
				} else {
					return false
				}
			}
		}
		return true
	}

	return docValue == value
}

// executeGroup groups documents and applies accumulators
func (c *Collection) executeGroup(docs []map[string]interface{}, group *GroupStage) ([]map[string]interface{}, error) {
	groups := make(map[interface{}][]map[string]interface{})

	// Group documents by _id
	for _, doc := range docs {
		key := resolveGroupKey(doc, group.ID)
		groups[key] = append(groups[key], doc)
	}

	// Apply accumulators to each group
	var results []map[string]interface{}
	for key, groupDocs := range groups {
		result := make(map[string]interface{})
		result["_id"] = key

		// Apply each accumulator
		for field, expr := range group.Sum {
			result[field] = accumulateSum(groupDocs, expr)
		}
		for field, expr := range group.Avg {
			result[field] = accumulateAvg(groupDocs, expr)
		}
		for field, expr := range group.Min {
			result[field] = accumulateMin(groupDocs, expr)
		}
		for field, expr := range group.Max {
			result[field] = accumulateMax(groupDocs, expr)
		}
		for field := range group.Count {
			result[field] = len(groupDocs)
		}
		for field, expr := range group.First {
			result[field] = accumulateFirst(groupDocs, expr)
		}
		for field, expr := range group.Last {
			result[field] = accumulateLast(groupDocs, expr)
		}
		for field, expr := range group.Push {
			result[field] = accumulatePush(groupDocs, expr)
		}
		for field, expr := range group.AddToSet {
			result[field] = accumulateAddToSet(groupDocs, expr)
		}

		results = append(results, result)
	}

	return results, nil
}

// resolveGroupKey resolves the grouping key for a document
func resolveGroupKey(doc map[string]interface{}, id interface{}) interface{} {
	switch v := id.(type) {
	case string:
		if strings.HasPrefix(v, "$") {
			return getFieldValue(doc, v[1:])
		}
		return v
	case map[string]interface{}:
		// Composite key
		key := make(map[string]interface{})
		for k, val := range v {
			if str, ok := val.(string); ok && strings.HasPrefix(str, "$") {
				key[k] = getFieldValue(doc, str[1:])
			} else {
				key[k] = val
			}
		}
		return key
	default:
		return v
	}
}

// Accumulator functions

func accumulateSum(docs []map[string]interface{}, expr interface{}) interface{} {
	var sum float64
	for _, doc := range docs {
		if val := resolveValue(doc, expr); val != nil {
			if num, ok := toFloat64(val); ok {
				sum += num
			}
		}
	}
	return sum
}

func accumulateAvg(docs []map[string]interface{}, expr interface{}) interface{} {
	var sum float64
	var count int
	for _, doc := range docs {
		if val := resolveValue(doc, expr); val != nil {
			if num, ok := toFloat64(val); ok {
				sum += num
				count++
			}
		}
	}
	if count == 0 {
		return nil
	}
	return sum / float64(count)
}

func accumulateMin(docs []map[string]interface{}, expr interface{}) interface{} {
	var min interface{}
	var minFloat float64
	hasMin := false

	for _, doc := range docs {
		if val := resolveValue(doc, expr); val != nil {
			if num, ok := toFloat64(val); ok {
				if !hasMin || num < minFloat {
					minFloat = num
					min = val
					hasMin = true
				}
			}
		}
	}
	return min
}

func accumulateMax(docs []map[string]interface{}, expr interface{}) interface{} {
	var max interface{}
	var maxFloat float64
	hasMax := false

	for _, doc := range docs {
		if val := resolveValue(doc, expr); val != nil {
			if num, ok := toFloat64(val); ok {
				if !hasMax || num > maxFloat {
					maxFloat = num
					max = val
					hasMax = true
				}
			}
		}
	}
	return max
}

func accumulateFirst(docs []map[string]interface{}, expr interface{}) interface{} {
	if len(docs) == 0 {
		return nil
	}
	return resolveValue(docs[0], expr)
}

func accumulateLast(docs []map[string]interface{}, expr interface{}) interface{} {
	if len(docs) == 0 {
		return nil
	}
	return resolveValue(docs[len(docs)-1], expr)
}

func accumulatePush(docs []map[string]interface{}, expr interface{}) interface{} {
	var results []interface{}
	for _, doc := range docs {
		results = append(results, resolveValue(doc, expr))
	}
	return results
}

func accumulateAddToSet(docs []map[string]interface{}, expr interface{}) interface{} {
	seen := make(map[string]bool)
	var results []interface{}
	for _, doc := range docs {
		val := resolveValue(doc, expr)
		key := fmt.Sprintf("%v", val)
		if !seen[key] {
			seen[key] = true
			results = append(results, val)
		}
	}
	return results
}

// executeSort sorts documents
func (c *Collection) executeSort(docs []map[string]interface{}, sortStage *SortStage) ([]map[string]interface{}, error) {
	// Convert SortStage.Fields to sortable format
	sortSpec := make([]sortField, 0)
	for field, order := range sortStage.Fields {
		sortSpec = append(sortSpec, sortField{field: field, order: order})
	}

	// Sort documents
	sort.Slice(docs, func(i, j int) bool {
		for _, sf := range sortSpec {
			vi := getFieldValue(docs[i], sf.field)
			vj := getFieldValue(docs[j], sf.field)
			
			cmp := compareValuesForSort(vi, vj)
			if cmp != 0 {
				if sf.order == 1 {
					return cmp < 0
				}
				return cmp > 0
			}
		}
		return false
	})

	return docs, nil
}

type sortField struct {
	field string
	order int
}

// executeProject reshapes documents
func (c *Collection) executeProject(docs []map[string]interface{}, project *ProjectStage) ([]map[string]interface{}, error) {
	var results []map[string]interface{}

	for _, doc := range docs {
		result := make(map[string]interface{})
		
		// Handle included fields
		if len(project.Included) > 0 {
			for _, field := range project.Included {
				if val, ok := getNestedField(doc, field); ok {
					result[field] = val
				}
			}
		} else if len(project.Excluded) > 0 {
			// Copy all except excluded
			for k, v := range doc {
				excluded := false
				for _, ex := range project.Excluded {
					if k == ex {
						excluded = true
						break
					}
				}
				if !excluded {
					result[k] = v
				}
			}
		}

		// Handle computed fields
		for field, expr := range project.Computed {
			result[field] = resolveValue(doc, expr)
		}

		// Handle Fields map
		for field, spec := range project.Fields {
			if b, ok := spec.(bool); ok {
				if b {
					if val, ok := getNestedField(doc, field); ok {
						result[field] = val
					}
				}
			} else if m, ok := spec.(map[string]interface{}); ok {
				// Handle expressions
				result[field] = evaluateExpression(doc, m)
			} else {
				result[field] = spec
			}
		}

		results = append(results, result)
	}

	return results, nil
}

// executeLimit limits the number of documents
func (c *Collection) executeLimit(docs []map[string]interface{}, limit int) ([]map[string]interface{}, error) {
	if limit >= len(docs) {
		return docs, nil
	}
	return docs[:limit], nil
}

// executeSkip skips documents
func (c *Collection) executeSkip(docs []map[string]interface{}, skip int) ([]map[string]interface{}, error) {
	if skip >= len(docs) {
		return []map[string]interface{}{}, nil
	}
	return docs[skip:], nil
}

// executeAddFields adds new fields to documents
func (c *Collection) executeAddFields(docs []map[string]interface{}, fields map[string]interface{}) ([]map[string]interface{}, error) {
	for _, doc := range docs {
		for field, expr := range fields {
			doc[field] = resolveValue(doc, expr)
		}
	}
	return docs, nil
}

// executeUnset removes fields from documents
func (c *Collection) executeUnset(docs []map[string]interface{}, unset interface{}) ([]map[string]interface{}, error) {
	var fields []string
	switch v := unset.(type) {
	case string:
		fields = []string{v}
	case []interface{}:
		for _, f := range v {
			if s, ok := f.(string); ok {
				fields = append(fields, s)
			}
		}
	case []string:
		fields = v
	}

	for _, doc := range docs {
		for _, field := range fields {
			delete(doc, field)
		}
	}
	return docs, nil
}

// executeLookup performs a left outer join
func (c *Collection) executeLookup(docs []map[string]interface{}, lookup *LookupStage) ([]map[string]interface{}, error) {
	// Get the foreign collection
	foreignCol, err := c.manager.GetCollection(lookup.From)
	if err != nil {
		// Collection doesn't exist, return docs with empty arrays
		for _, doc := range docs {
			doc[lookup.As] = []interface{}{}
		}
		return docs, nil
	}

	foreignDocs, err := foreignCol.FindAll()
	if err != nil {
		return nil, err
	}

	for _, doc := range docs {
		localValue := getFieldValue(doc, lookup.LocalField)
		var matches []interface{}

		for _, foreignDoc := range foreignDocs {
			foreignValue := getFieldValue(foreignDoc.Data, lookup.ForeignField)
			if localValue == foreignValue {
				matches = append(matches, foreignDoc.Data)
			}
		}

		if matches == nil {
			matches = []interface{}{}
		}
		doc[lookup.As] = matches
	}

	return docs, nil
}

// executeUnwind deconstructs an array field
func (c *Collection) executeUnwind(docs []map[string]interface{}, unwind *UnwindStage) ([]map[string]interface{}, error) {
	path := unwind.Path
	if strings.HasPrefix(path, "$") {
		path = path[1:]
	}

	var results []map[string]interface{}

	for _, doc := range docs {
		arr, ok := getNestedField(doc, path)
		if !ok {
			if unwind.PreserveNullAndEmptyArrays {
				results = append(results, doc)
			}
			continue
		}

		arrValue, ok := arr.([]interface{})
		if !ok {
			if unwind.PreserveNullAndEmptyArrays {
				results = append(results, doc)
			}
			continue
		}

		if len(arrValue) == 0 {
			if unwind.PreserveNullAndEmptyArrays {
				results = append(results, doc)
			}
			continue
		}

		for i, item := range arrValue {
			newDoc := copyMap(doc)
			setNestedField(newDoc, path, item)
			
			if unwind.IncludeArrayIndex != "" {
				newDoc[unwind.IncludeArrayIndex] = i
			}
			
			results = append(results, newDoc)
		}
	}

	return results, nil
}

// executeCount counts documents
func (c *Collection) executeCount(docs []map[string]interface{}, countField string) ([]map[string]interface{}, error) {
	return []map[string]interface{}{{countField: len(docs)}}, nil
}

// executeFacet processes multiple pipelines
func (c *Collection) executeFacet(docs []map[string]interface{}, facet map[string][]PipelineStage) ([]map[string]interface{}, error) {
	result := make(map[string]interface{})

	for name, stages := range facet {
		// Create a temporary collection with the same storage
		tempCol := &Collection{
			Name:    c.Name,
			Schema:  c.Schema,
			storage: c.storage,
			indexes: c.indexes,
			manager: c.manager,
		}

		pipeline := &AggregationPipeline{stages: stages}
		facetResults, err := tempCol.ExecuteAggregation(pipeline)
		if err != nil {
			return nil, err
		}
		result[name] = facetResults
	}

	return []map[string]interface{}{result}, nil
}

// executeBucket categorizes documents into groups
func (c *Collection) executeBucket(docs []map[string]interface{}, bucket *BucketStage) ([]map[string]interface{}, error) {
	buckets := make(map[interface{}][]map[string]interface{})
	var defaultBucket []map[string]interface{}

	for _, doc := range docs {
		value := resolveValue(doc, bucket.GroupBy)
		bucketKey := findBucket(value, bucket.Boundaries)
		
		if bucketKey == nil {
			defaultBucket = append(defaultBucket, doc)
		} else {
			buckets[bucketKey] = append(buckets[bucketKey], doc)
		}
	}

	var results []map[string]interface{}

	// Create bucket results
	for i := 0; i < len(bucket.Boundaries)-1; i++ {
		result := make(map[string]interface{})
		result["_id"] = bucket.Boundaries[i]
		result["count"] = len(buckets[bucket.Boundaries[i]])
		
		// Apply output expressions
		for field, expr := range bucket.Output {
			result[field] = applyAccumulator(buckets[bucket.Boundaries[i]], expr)
		}
		
		results = append(results, result)
	}

	// Add default bucket if specified
	if bucket.Default != nil && len(defaultBucket) > 0 {
		result := make(map[string]interface{})
		result["_id"] = bucket.Default
		result["count"] = len(defaultBucket)
		
		for field, expr := range bucket.Output {
			result[field] = applyAccumulator(defaultBucket, expr)
		}
		
		results = append(results, result)
	}

	return results, nil
}

// executeSortByCount groups and sorts by count
func (c *Collection) executeSortByCount(docs []map[string]interface{}, sortByCount *SortByCountStage) ([]map[string]interface{}, error) {
	field := sortByCount.Field
	if strings.HasPrefix(field, "$") {
		field = field[1:]
	}

	counts := make(map[interface{}]int)
	for _, doc := range docs {
		value := getFieldValue(doc, field)
		counts[value]++
	}

	// Convert to slice and sort by count descending
	type countEntry struct {
		key   interface{}
		count int
	}
	
	var entries []countEntry
	for k, v := range counts {
		entries = append(entries, countEntry{key: k, count: v})
	}

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].count > entries[j].count
	})

	var results []map[string]interface{}
	for _, entry := range entries {
		results = append(results, map[string]interface{}{
			"_id":   entry.key,
			"count": entry.count,
		})
	}

	return results, nil
}

// executeReplaceRoot replaces document with a nested object
func (c *Collection) executeReplaceRoot(docs []map[string]interface{}, replaceRoot interface{}) ([]map[string]interface{}, error) {
	var results []map[string]interface{}

	for _, doc := range docs {
		newRoot := resolveValue(doc, replaceRoot)
		if m, ok := newRoot.(map[string]interface{}); ok {
			results = append(results, m)
		}
	}

	return results, nil
}

// Helper functions

func getFieldValue(doc map[string]interface{}, field string) interface{} {
	val, _ := getNestedField(doc, field)
	return val
}

func getNestedField(doc map[string]interface{}, field string) (interface{}, bool) {
	parts := strings.Split(field, ".")
	var current interface{} = doc

	for _, part := range parts {
		switch v := current.(type) {
		case map[string]interface{}:
			var ok bool
			current, ok = v[part]
			if !ok {
				return nil, false
			}
		default:
			return nil, false
		}
	}

	return current, true
}

func setNestedField(doc map[string]interface{}, field string, value interface{}) {
	parts := strings.Split(field, ".")
	current := doc

	for i, part := range parts {
		if i == len(parts)-1 {
			current[part] = value
			return
		}

		if next, ok := current[part].(map[string]interface{}); ok {
			current = next
		} else {
			next = make(map[string]interface{})
			current[part] = next
			current = next
		}
	}
}

func resolveValue(doc map[string]interface{}, expr interface{}) interface{} {
	switch v := expr.(type) {
	case string:
		if strings.HasPrefix(v, "$") {
			return getFieldValue(doc, v[1:])
		}
		return v
	case map[string]interface{}:
		return evaluateExpression(doc, v)
	default:
		return expr
	}
}

func evaluateExpression(doc map[string]interface{}, expr map[string]interface{}) interface{} {
	for op, val := range expr {
		switch op {
		case "$sum":
			return accumulateSum([]map[string]interface{}{doc}, val)
		case "$avg":
			return accumulateAvg([]map[string]interface{}{doc}, val)
		case "$min":
			return accumulateMin([]map[string]interface{}{doc}, val)
		case "$max":
			return accumulateMax([]map[string]interface{}{doc}, val)
		case "$add":
			return arithmeticOp(doc, val, "+")
		case "$subtract":
			return arithmeticOp(doc, val, "-")
		case "$multiply":
			return arithmeticOp(doc, val, "*")
		case "$divide":
			return arithmeticOp(doc, val, "/")
		case "$mod":
			return arithmeticOp(doc, val, "%")
		case "$concat":
			return concatenate(doc, val)
		case "$substr":
			return substring(doc, val)
		case "$toLower":
			return toLower(doc, val)
		case "$toUpper":
			return toUpper(doc, val)
		case "$size":
			return arraySize(doc, val)
		case "$arrayElemAt":
			return arrayElemAt(doc, val)
		case "$cond":
			return conditional(doc, val)
		case "$ifNull":
			return ifNull(doc, val)
		case "$type":
			return getType(doc, val)
		case "$toString":
			return toString(doc, val)
		case "$toInt":
			return toInt(doc, val)
		case "$toDouble":
			return toDouble(doc, val)
		case "$toBool":
			return toBool(doc, val)
		case "$year", "$month", "$dayOfMonth", "$hour", "$minute", "$second":
			return datePart(doc, op, val)
		}
	}
	return expr
}

func arithmeticOp(doc map[string]interface{}, val interface{}, op string) interface{} {
	arr, ok := val.([]interface{})
	if !ok || len(arr) < 2 {
		return nil
	}

	a := resolveValue(doc, arr[0])
	b := resolveValue(doc, arr[1])

	an, aok := toFloat64(a)
	bn, bok := toFloat64(b)

	if !aok || !bok {
		return nil
	}

	switch op {
	case "+":
		return an + bn
	case "-":
		return an - bn
	case "*":
		return an * bn
	case "/":
		if bn == 0 {
			return nil
		}
		return an / bn
	case "%":
		return math.Mod(an, bn)
	}
	return nil
}

func concatenate(doc map[string]interface{}, val interface{}) interface{} {
	arr, ok := val.([]interface{})
	if !ok {
		return nil
	}

	var result string
	for _, v := range arr {
		if s, ok := resolveValue(doc, v).(string); ok {
			result += s
		}
	}
	return result
}

func substring(doc map[string]interface{}, val interface{}) interface{} {
	arr, ok := val.([]interface{})
	if !ok || len(arr) < 3 {
		return nil
	}

	str, ok := resolveValue(doc, arr[0]).(string)
	if !ok {
		return nil
	}

	start, sok := toIntValue(resolveValue(doc, arr[1]))
	length, lok := toIntValue(resolveValue(doc, arr[2]))

	if !sok || !lok {
		return nil
	}

	if start < 0 || start > len(str) {
		return nil
	}

	end := start + length
	if end > len(str) {
		end = len(str)
	}

	return str[start:end]
}

func toLower(doc map[string]interface{}, val interface{}) interface{} {
	if str, ok := resolveValue(doc, val).(string); ok {
		return strings.ToLower(str)
	}
	return nil
}

func toUpper(doc map[string]interface{}, val interface{}) interface{} {
	if str, ok := resolveValue(doc, val).(string); ok {
		return strings.ToUpper(str)
	}
	return nil
}

func arraySize(doc map[string]interface{}, val interface{}) interface{} {
	if arr, ok := resolveValue(doc, val).([]interface{}); ok {
		return len(arr)
	}
	return nil
}

func arrayElemAt(doc map[string]interface{}, val interface{}) interface{} {
	arr, ok := val.([]interface{})
	if !ok || len(arr) < 2 {
		return nil
	}

	array, aok := resolveValue(doc, arr[0]).([]interface{})
	idx, iok := toIntValue(resolveValue(doc, arr[1]))

	if !aok || !iok {
		return nil
	}

	if idx < 0 {
		idx = len(array) + idx
	}

	if idx < 0 || idx >= len(array) {
		return nil
	}

	return array[idx]
}

func conditional(doc map[string]interface{}, val interface{}) interface{} {
	m, ok := val.(map[string]interface{})
	if !ok {
		return nil
	}

	cond := m["cond"]
	then := m["then"]
	els := m["else"]

	if evaluateCondition(doc, cond) {
		return resolveValue(doc, then)
	}
	return resolveValue(doc, els)
}

func evaluateCondition(doc map[string]interface{}, cond interface{}) bool {
	switch v := cond.(type) {
	case bool:
		return v
	case map[string]interface{}:
		for op, val := range v {
			switch op {
			case "$eq":
				arr, ok := val.([]interface{})
				if ok && len(arr) == 2 {
					return resolveValue(doc, arr[0]) == resolveValue(doc, arr[1])
				}
			case "$ne":
				arr, ok := val.([]interface{})
				if ok && len(arr) == 2 {
					return resolveValue(doc, arr[0]) != resolveValue(doc, arr[1])
				}
			case "$gt", "$gte", "$lt", "$lte":
				arr, ok := val.([]interface{})
				if ok && len(arr) == 2 {
					return compareOperator(resolveValue(doc, arr[0]), resolveValue(doc, arr[1]), op)
				}
			case "$and":
				arr, ok := val.([]interface{})
				if ok {
					for _, c := range arr {
						if !evaluateCondition(doc, c) {
							return false
						}
					}
					return true
				}
			case "$or":
				arr, ok := val.([]interface{})
				if ok {
					for _, c := range arr {
						if evaluateCondition(doc, c) {
							return true
						}
					}
					return false
				}
			case "$not":
				return !evaluateCondition(doc, val)
			}
		}
	}
	return false
}

func compareOperator(a, b interface{}, op string) bool {
	an, aok := toFloat64(a)
	bn, bok := toFloat64(b)
	if !aok || !bok {
		return false
	}

	switch op {
	case "$gt":
		return an > bn
	case "$gte":
		return an >= bn
	case "$lt":
		return an < bn
	case "$lte":
		return an <= bn
	}
	return false
}

func ifNull(doc map[string]interface{}, val interface{}) interface{} {
	arr, ok := val.([]interface{})
	if !ok || len(arr) < 2 {
		return nil
	}

	first := resolveValue(doc, arr[0])
	if first != nil {
		return first
	}
	return resolveValue(doc, arr[1])
}

func getType(doc map[string]interface{}, val interface{}) interface{} {
	v := resolveValue(doc, val)
	switch v.(type) {
	case string:
		return "string"
	case int, int32, int64, float32, float64:
		return "number"
	case bool:
		return "bool"
	case []interface{}:
		return "array"
	case map[string]interface{}:
		return "object"
	case nil:
		return "null"
	default:
		return "unknown"
	}
}

func toString(doc map[string]interface{}, val interface{}) interface{} {
	v := resolveValue(doc, val)
	return fmt.Sprintf("%v", v)
}

func toInt(doc map[string]interface{}, val interface{}) interface{} {
	v := resolveValue(doc, val)
	if n, ok := toFloat64(v); ok {
		return int(n)
	}
	return nil
}

func toDouble(doc map[string]interface{}, val interface{}) interface{} {
	v := resolveValue(doc, val)
	if n, ok := toFloat64(v); ok {
		return n
	}
	return nil
}

func toBool(doc map[string]interface{}, val interface{}) interface{} {
	v := resolveValue(doc, val)
	switch x := v.(type) {
	case bool:
		return x
	case int, int32, int64, float32, float64:
		return x != 0
	case string:
		return x != ""
	default:
		return false
	}
}

func datePart(doc map[string]interface{}, op string, val interface{}) interface{} {
	// Simplified date extraction - would need proper date parsing
	return nil
}

func toFloat64(val interface{}) (float64, bool) {
	switch v := val.(type) {
	case int:
		return float64(v), true
	case int32:
		return float64(v), true
	case int64:
		return float64(v), true
	case float32:
		return float64(v), true
	case float64:
		return v, true
	default:
		return 0, false
	}
}

func toIntValue(val interface{}) (int, bool) {
	switch v := val.(type) {
	case int:
		return v, true
	case int32:
		return int(v), true
	case int64:
		return int(v), true
	case float32:
		return int(v), true
	case float64:
		return int(v), true
	default:
		return 0, false
	}
}

func compareGreater(doc map[string]interface{}, field string, value interface{}) bool {
	docValue := getFieldValue(doc, field)
	return compareValues(docValue, value, ">")
}

func compareGreaterOrEqual(doc map[string]interface{}, field string, value interface{}) bool {
	docValue := getFieldValue(doc, field)
	return compareValues(docValue, value, ">=")
}

func compareLess(doc map[string]interface{}, field string, value interface{}) bool {
	docValue := getFieldValue(doc, field)
	return compareValues(docValue, value, "<")
}

func compareLessOrEqual(doc map[string]interface{}, field string, value interface{}) bool {
	docValue := getFieldValue(doc, field)
	return compareValues(docValue, value, "<=")
}

func compareValues(a, b interface{}, op string) bool {
	an, aok := toFloat64(a)
	bn, bok := toFloat64(b)

	if aok && bok {
		switch op {
		case ">":
			return an > bn
		case ">=":
			return an >= bn
		case "<":
			return an < bn
		case "<=":
			return an <= bn
		}
	}

	// String comparison
	as, aok := a.(string)
	bs, bok := b.(string)

	if aok && bok {
		switch op {
		case ">":
			return as > bs
		case ">=":
			return as >= bs
		case "<":
			return as < bs
		case "<=":
			return as <= bs
		}
	}

	return false
}

func compareValuesForSort(a, b interface{}) int {
	an, aok := toFloat64(a)
	bn, bok := toFloat64(b)

	if aok && bok {
		if an < bn {
			return -1
		} else if an > bn {
			return 1
		}
		return 0
	}

	// String comparison
	as, aok := a.(string)
	bs, bok := b.(string)

	if aok && bok {
		if as < bs {
			return -1
		} else if as > bs {
			return 1
		}
		return 0
	}

	// Null handling
	if a == nil && b != nil {
		return -1
	}
	if a != nil && b == nil {
		return 1
	}

	return 0
}

func isInArray(doc map[string]interface{}, field string, values []interface{}) bool {
	docValue := getFieldValue(doc, field)
	for _, v := range values {
		if docValue == v {
			return true
		}
	}
	return false
}

func containsValue(arr []interface{}, val interface{}) bool {
	for _, v := range arr {
		if v == val {
			return true
		}
	}
	return false
}

func findBucket(value interface{}, boundaries []interface{}) interface{} {
	val, ok := toFloat64(value)
	if !ok {
		return nil
	}

	for i := 0; i < len(boundaries)-1; i++ {
		low, lok := toFloat64(boundaries[i])
		high, hok := toFloat64(boundaries[i+1])

		if lok && hok && val >= low && val < high {
			return boundaries[i]
		}
	}

	return nil
}

func applyAccumulator(docs []map[string]interface{}, expr interface{}) interface{} {
	if m, ok := expr.(map[string]interface{}); ok {
		for acc, val := range m {
			switch acc {
			case "$sum":
				return accumulateSum(docs, val)
			case "$avg":
				return accumulateAvg(docs, val)
			case "$min":
				return accumulateMin(docs, val)
			case "$max":
				return accumulateMax(docs, val)
			case "$count":
				return len(docs)
			case "$first":
				return accumulateFirst(docs, val)
			case "$last":
				return accumulateLast(docs, val)
			case "$push":
				return accumulatePush(docs, val)
			case "$addToSet":
				return accumulateAddToSet(docs, val)
			}
		}
	}
	return nil
}

func copyMap(m map[string]interface{}) map[string]interface{} {
	result := make(map[string]interface{})
	for k, v := range m {
		result[k] = v
	}
	return result
}
