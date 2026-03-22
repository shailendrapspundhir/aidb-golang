package api

import (
	"aidb/internal/auth"
	"aidb/internal/collection"
	"aidb/internal/rbac"
	"net/http"
)

// AggregationRequest represents an aggregation pipeline request
type AggregationRequest struct {
	Pipeline []map[string]interface{} `json:"pipeline"`
}

// AggregationResponse represents the response from an aggregation query
type AggregationResponse struct {
	Success bool                      `json:"success"`
	Data    []map[string]interface{}  `json:"data,omitempty"`
	Error   string                    `json:"error,omitempty"`
	Count   int                       `json:"count,omitempty"`
}

// handleAggregation handles aggregation pipeline requests
func (h *Handler) handleAggregation(w http.ResponseWriter, r *http.Request) {
	collectionName := r.PathValue("name")
	
	col, err := h.collectionManager.GetCollection(collectionName)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}

	// Parse the request
	var req AggregationRequest
	if err := parseJSONBody(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body: "+err.Error())
		return
	}

	if len(req.Pipeline) == 0 {
		writeError(w, http.StatusBadRequest, "pipeline is required and must have at least one stage")
		return
	}

	// Check RBAC permissions
	user := r.Context().Value(UserContextKey).(*auth.User)
	rbacCtx := rbac.RequestContext{
		TenantID:   user.TenantID,
		Collection: collectionName,
		Action:     rbac.ActionRead, // Aggregation is a read operation
	}
	
	allowed, err := h.enforcer.Enforce(user.Roles, user.TenantID, rbacCtx)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "rbac error: "+err.Error())
		return
	}
	if !allowed {
		writeError(w, http.StatusForbidden, "access denied")
		return
	}

	// Parse pipeline stages
	pipeline := &collection.AggregationPipeline{}
	for _, stageMap := range req.Pipeline {
		stage, err := parsePipelineStage(stageMap)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid pipeline stage: "+err.Error())
			return
		}
		pipeline.AddStage(stage)
	}

	// Execute aggregation
	results, err := col.ExecuteAggregation(pipeline)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "aggregation failed: "+err.Error())
		return
	}

	// Filter fields based on permission
	allowedFields, err := h.enforcer.GetAllowedFields(user.Roles, user.TenantID, rbacCtx)
	if err == nil && allowedFields != nil {
		isAll := false
		for _, f := range allowedFields {
			if f == "*" {
				isAll = true
				break
			}
		}

		if !isAll {
			for i, doc := range results {
				newDoc := make(map[string]interface{})
				for _, f := range allowedFields {
					if v, ok := doc[f]; ok {
						newDoc[f] = v
					}
				}
				results[i] = newDoc
			}
		}
	}

	writeSuccess(w, map[string]interface{}{
		"results": results,
		"count":   len(results),
	})
}

// parsePipelineStage parses a map into a PipelineStage
func parsePipelineStage(m map[string]interface{}) (collection.PipelineStage, error) {
	stage := collection.PipelineStage{}

	for key, value := range m {
		switch key {
		case "$match":
			matchStage, err := parseMatchStage(value)
			if err != nil {
				return stage, err
			}
			stage.Match = matchStage

		case "$group":
			groupStage, err := parseGroupStage(value)
			if err != nil {
				return stage, err
			}
			stage.Group = groupStage

		case "$sort":
			sortStage, err := parseSortStage(value)
			if err != nil {
				return stage, err
			}
			stage.Sort = sortStage

		case "$project":
			projectStage, err := parseProjectStage(value)
			if err != nil {
				return stage, err
			}
			stage.Project = projectStage

		case "$limit":
			if n, ok := value.(float64); ok {
				lim := int(n)
				stage.Limit = &lim
			}

		case "$skip":
			if n, ok := value.(float64); ok {
				skip := int(n)
				stage.Skip = &skip
			}

		case "$addFields":
			if fields, ok := value.(map[string]interface{}); ok {
				stage.AddFields = fields
			}

		case "$unset":
			stage.Unset = value

		case "$lookup":
			lookupStage, err := parseLookupStage(value)
			if err != nil {
				return stage, err
			}
			stage.Lookup = lookupStage

		case "$unwind":
			unwindStage, err := parseUnwindStage(value)
			if err != nil {
				return stage, err
			}
			stage.Unwind = unwindStage

		case "$count":
			if s, ok := value.(string); ok {
				stage.Count = s
			}

		case "$facet":
			if facetMap, ok := value.(map[string]interface{}); ok {
				facet := make(map[string][]collection.PipelineStage)
				for name, pipeline := range facetMap {
					if pipelineArr, ok := pipeline.([]interface{}); ok {
						var stages []collection.PipelineStage
						for _, s := range pipelineArr {
							if stageMap, ok := s.(map[string]interface{}); ok {
								parsedStage, err := parsePipelineStage(stageMap)
								if err != nil {
									return stage, err
								}
								stages = append(stages, parsedStage)
							}
						}
						facet[name] = stages
					}
				}
				stage.Facet = facet
			}

		case "$bucket":
			bucketStage, err := parseBucketStage(value)
			if err != nil {
				return stage, err
			}
			stage.Bucket = bucketStage

		case "$sortByCount":
			sortByCountStage, err := parseSortByCountStage(value)
			if err != nil {
				return stage, err
			}
			stage.SortByCount = sortByCountStage

		case "$replaceRoot":
			stage.ReplaceRoot = value

		case "$set":
			if fields, ok := value.(map[string]interface{}); ok {
				stage.Set = fields
			}
		}
	}

	return stage, nil
}

// parseMatchStage parses a $match stage
func parseMatchStage(value interface{}) (*collection.MatchStage, error) {
	m, ok := value.(map[string]interface{})
	if !ok {
		return nil, nil
	}

	match := &collection.MatchStage{
		Fields: make(map[string]interface{}),
	}

	for key, val := range m {
		switch key {
		case "$and":
			if arr, ok := val.([]interface{}); ok {
				for _, item := range arr {
					if condMap, ok := item.(map[string]interface{}); ok {
						match.And = append(match.And, condMap)
					}
				}
			}
		case "$or":
			if arr, ok := val.([]interface{}); ok {
				for _, item := range arr {
					if condMap, ok := item.(map[string]interface{}); ok {
						match.Or = append(match.Or, condMap)
					}
				}
			}
		case "$nor":
			if arr, ok := val.([]interface{}); ok {
				for _, item := range arr {
					if condMap, ok := item.(map[string]interface{}); ok {
						match.Nor = append(match.Nor, condMap)
					}
				}
			}
		case "$eq":
			if eqMap, ok := val.(map[string]interface{}); ok {
				match.Eq = eqMap
			}
		case "$ne":
			if neMap, ok := val.(map[string]interface{}); ok {
				match.Ne = neMap
			}
		case "$gt":
			if gtMap, ok := val.(map[string]interface{}); ok {
				match.Gt = gtMap
			}
		case "$gte":
			if gteMap, ok := val.(map[string]interface{}); ok {
				match.Gte = gteMap
			}
		case "$lt":
			if ltMap, ok := val.(map[string]interface{}); ok {
				match.Lt = ltMap
			}
		case "$lte":
			if lteMap, ok := val.(map[string]interface{}); ok {
				match.Lte = lteMap
			}
		case "$in":
			if inMap, ok := val.(map[string]interface{}); ok {
				match.In = make(map[string][]interface{})
				for k, v := range inMap {
					if arr, ok := v.([]interface{}); ok {
						match.In[k] = arr
					}
				}
			}
		case "$nin":
			if ninMap, ok := val.(map[string]interface{}); ok {
				match.Nin = make(map[string][]interface{})
				for k, v := range ninMap {
					if arr, ok := v.([]interface{}); ok {
						match.Nin[k] = arr
					}
				}
			}
		case "$exists":
			if existsMap, ok := val.(map[string]interface{}); ok {
				match.Exists = make(map[string]bool)
				for k, v := range existsMap {
					if b, ok := v.(bool); ok {
						match.Exists[k] = b
					}
				}
			}
		case "$regex":
			if regexMap, ok := val.(map[string]interface{}); ok {
				match.Regex = make(map[string]string)
				for k, v := range regexMap {
					if s, ok := v.(string); ok {
						match.Regex[k] = s
					}
				}
			}
		default:
			// Regular field match
			match.Fields[key] = val
		}
	}

	return match, nil
}

// parseGroupStage parses a $group stage
func parseGroupStage(value interface{}) (*collection.GroupStage, error) {
	m, ok := value.(map[string]interface{})
	if !ok {
		return nil, nil
	}

	group := &collection.GroupStage{}

	for key, val := range m {
		switch key {
		case "_id":
			group.ID = val
		default:
			// This is an accumulator field
			if accMap, ok := val.(map[string]interface{}); ok {
				for acc, accVal := range accMap {
					switch acc {
					case "$sum":
						if group.Sum == nil {
							group.Sum = make(map[string]interface{})
						}
						group.Sum[key] = accVal
					case "$avg":
						if group.Avg == nil {
							group.Avg = make(map[string]interface{})
						}
						group.Avg[key] = accVal
					case "$min":
						if group.Min == nil {
							group.Min = make(map[string]interface{})
						}
						group.Min[key] = accVal
					case "$max":
						if group.Max == nil {
							group.Max = make(map[string]interface{})
						}
						group.Max[key] = accVal
					case "$count":
						if group.Count == nil {
							group.Count = make(map[string]interface{})
						}
						group.Count[key] = true
					case "$first":
						if group.First == nil {
							group.First = make(map[string]interface{})
						}
						group.First[key] = accVal
					case "$last":
						if group.Last == nil {
							group.Last = make(map[string]interface{})
						}
						group.Last[key] = accVal
					case "$push":
						if group.Push == nil {
							group.Push = make(map[string]interface{})
						}
						group.Push[key] = accVal
					case "$addToSet":
						if group.AddToSet == nil {
							group.AddToSet = make(map[string]interface{})
						}
						group.AddToSet[key] = accVal
					}
				}
			}
		}
	}

	return group, nil
}

// parseSortStage parses a $sort stage
func parseSortStage(value interface{}) (*collection.SortStage, error) {
	m, ok := value.(map[string]interface{})
	if !ok {
		return nil, nil
	}

	sort := &collection.SortStage{
		Fields: make(map[string]int),
	}

	for key, val := range m {
		if n, ok := val.(float64); ok {
			sort.Fields[key] = int(n)
		}
	}

	return sort, nil
}

// parseProjectStage parses a $project stage
func parseProjectStage(value interface{}) (*collection.ProjectStage, error) {
	m, ok := value.(map[string]interface{})
	if !ok {
		return nil, nil
	}

	project := &collection.ProjectStage{
		Fields:   make(map[string]interface{}),
		Included: []string{},
		Excluded: []string{},
		Computed: make(map[string]interface{}),
	}

	for key, val := range m {
		project.Fields[key] = val
		
		if b, ok := val.(bool); ok {
			if b {
				project.Included = append(project.Included, key)
			} else {
				project.Excluded = append(project.Excluded, key)
			}
		} else if _, ok := val.(map[string]interface{}); ok {
			project.Computed[key] = val
		}
	}

	return project, nil
}

// parseLookupStage parses a $lookup stage
func parseLookupStage(value interface{}) (*collection.LookupStage, error) {
	m, ok := value.(map[string]interface{})
	if !ok {
		return nil, nil
	}

	lookup := &collection.LookupStage{}

	if from, ok := m["from"].(string); ok {
		lookup.From = from
	}
	if localField, ok := m["localField"].(string); ok {
		lookup.LocalField = localField
	}
	if foreignField, ok := m["foreignField"].(string); ok {
		lookup.ForeignField = foreignField
	}
	if as, ok := m["as"].(string); ok {
		lookup.As = as
	}
	if let, ok := m["let"].(map[string]interface{}); ok {
		lookup.Let = let
	}
	if pipeline, ok := m["pipeline"].([]interface{}); ok {
		for _, s := range pipeline {
			if stageMap, ok := s.(map[string]interface{}); ok {
				parsedStage, err := parsePipelineStage(stageMap)
				if err != nil {
					return nil, err
				}
				lookup.Pipeline = append(lookup.Pipeline, parsedStage)
			}
		}
	}

	return lookup, nil
}

// parseUnwindStage parses a $unwind stage
func parseUnwindStage(value interface{}) (*collection.UnwindStage, error) {
	// $unwind can be a string or an object
	if s, ok := value.(string); ok {
		return &collection.UnwindStage{Path: s}, nil
	}

	m, ok := value.(map[string]interface{})
	if !ok {
		return nil, nil
	}

	unwind := &collection.UnwindStage{}

	if path, ok := m["path"].(string); ok {
		unwind.Path = path
	}
	if includeArrayIndex, ok := m["includeArrayIndex"].(string); ok {
		unwind.IncludeArrayIndex = includeArrayIndex
	}
	if preserveNullAndEmptyArrays, ok := m["preserveNullAndEmptyArrays"].(bool); ok {
		unwind.PreserveNullAndEmptyArrays = preserveNullAndEmptyArrays
	}

	return unwind, nil
}

// parseBucketStage parses a $bucket stage
func parseBucketStage(value interface{}) (*collection.BucketStage, error) {
	m, ok := value.(map[string]interface{})
	if !ok {
		return nil, nil
	}

	bucket := &collection.BucketStage{}

	if groupBy, ok := m["groupBy"]; ok {
		bucket.GroupBy = groupBy
	}
	if boundaries, ok := m["boundaries"].([]interface{}); ok {
		bucket.Boundaries = boundaries
	}
	if defaultVal, ok := m["default"]; ok {
		bucket.Default = defaultVal
	}
	if output, ok := m["output"].(map[string]interface{}); ok {
		bucket.Output = output
	}

	return bucket, nil
}

// parseSortByCountStage parses a $sortByCount stage
func parseSortByCountStage(value interface{}) (*collection.SortByCountStage, error) {
	if s, ok := value.(string); ok {
		return &collection.SortByCountStage{Field: s}, nil
	}
	return nil, nil
}

// handleDistinct handles distinct value queries
func (h *Handler) handleDistinct(w http.ResponseWriter, r *http.Request) {
	collectionName := r.PathValue("name")
	field := r.PathValue("field")

	col, err := h.collectionManager.GetCollection(collectionName)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}

	// Check RBAC permissions
	user := r.Context().Value(UserContextKey).(*auth.User)
	rbacCtx := rbac.RequestContext{
		TenantID:   user.TenantID,
		Collection: collectionName,
		Action:     rbac.ActionRead,
	}

	allowed, err := h.enforcer.Enforce(user.Roles, user.TenantID, rbacCtx)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "rbac error: "+err.Error())
		return
	}
	if !allowed {
		writeError(w, http.StatusForbidden, "access denied")
		return
	}

	// Get all documents
	docs, err := col.FindAll()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to get documents: "+err.Error())
		return
	}

	// Collect distinct values
	seen := make(map[interface{}]bool)
	var distinct []interface{}
	for _, doc := range docs {
		if val, ok := doc.Data[field]; ok {
			if !seen[val] {
				seen[val] = true
				distinct = append(distinct, val)
			}
		}
	}

	writeSuccess(w, map[string]interface{}{
		"values": distinct,
		"count":  len(distinct),
	})
}

// handleStats handles collection statistics
func (h *Handler) handleStats(w http.ResponseWriter, r *http.Request) {
	collectionName := r.PathValue("name")

	col, err := h.collectionManager.GetCollection(collectionName)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}

	// Check RBAC permissions
	user := r.Context().Value(UserContextKey).(*auth.User)
	rbacCtx := rbac.RequestContext{
		TenantID:   user.TenantID,
		Collection: collectionName,
		Action:     rbac.ActionRead,
	}

	allowed, err := h.enforcer.Enforce(user.Roles, user.TenantID, rbacCtx)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "rbac error: "+err.Error())
		return
	}
	if !allowed {
		writeError(w, http.StatusForbidden, "access denied")
		return
	}

	// Get collection stats
	indexes := col.GetIndexes()
	indexList := make([]map[string]interface{}, 0, len(indexes))
	for field, idx := range indexes {
		indexList = append(indexList, map[string]interface{}{
			"field": field,
			"type":  idx.Type(),
			"count": idx.Count(),
		})
	}

	writeSuccess(w, map[string]interface{}{
		"collection": collectionName,
		"count":      col.Count(),
		"hasSchema":  col.Schema != nil,
		"indexes":    indexList,
	})
}
