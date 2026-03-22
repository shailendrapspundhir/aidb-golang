package api

import (
	"encoding/json"
	"net/http"
)

// HandleSwaggerJSON serves the OpenAPI JSON specification
func HandleSwaggerJSON(w http.ResponseWriter, r *http.Request) {
	spec, err := GetSwaggerJSON()
	if err != nil {
		http.Error(w, "Failed to load OpenAPI spec", http.StatusInternalServerError)
		return
	}
	
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Write(spec)
}

// HandleSwaggerUI serves the Swagger UI HTML
func HandleSwaggerUI(w http.ResponseWriter, r *http.Request) {
	html := `<!DOCTYPE html>
<html>
<head>
    <title>AIDB API Documentation</title>
    <link rel="stylesheet" type="text/css" href="https://unpkg.com/swagger-ui-dist@5.9.0/swagger-ui.css">
    <style>
        html { box-sizing: border-box; overflow: -moz-scrollbars-vertical; overflow-y: scroll; }
        *, *:before, *:after { box-sizing: inherit; }
        body { margin:0; background: #fafafa; }
    </style>
</head>
<body>
    <div id="swagger-ui"></div>
    <script src="https://unpkg.com/swagger-ui-dist@5.9.0/swagger-ui-bundle.js"></script>
    <script src="https://unpkg.com/swagger-ui-dist@5.9.0/swagger-ui-standalone-preset.js"></script>
    <script>
    window.onload = function() {
        const ui = SwaggerUIBundle({
            url: "/api/v1/swagger.json",
            dom_id: '#swagger-ui',
            deepLinking: true,
            presets: [
                SwaggerUIBundle.presets.apis,
                SwaggerUIStandalonePreset
            ],
            plugins: [
                SwaggerUIBundle.plugins.DownloadUrl
            ],
            layout: "StandaloneLayout"
        })
        window.ui = ui
    }
    </script>
</body>
</html>`
	
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write([]byte(html))
}

// OpenAPI Specification
const openAPISpec = `{
  "openapi": "3.0.3",
  "info": {
    "title": "AIDB API",
    "description": "A MongoDB-like document database with vector search capabilities. AIDB provides a RESTful API for managing collections, documents, indexes, and running aggregation pipelines similar to MongoDB's aggregation framework.",
    "version": "1.0.0",
    "contact": {
      "name": "AIDB Support"
    },
    "license": {
      "name": "MIT"
    }
  },
  "servers": [
    {
      "url": "http://localhost:8080/api/v1",
      "description": "Local development server"
    }
  ],
  "security": [
    {
      "BearerAuth": []
    }
  ],
  "components": {
    "securitySchemes": {
      "BearerAuth": {
        "type": "http",
        "scheme": "bearer",
        "description": "JWT token or API key authentication"
      }
    },
    "schemas": {
      "Error": {
        "type": "object",
        "properties": {
          "success": {
            "type": "boolean",
            "example": false
          },
          "error": {
            "type": "string",
            "example": "Error message"
          }
        }
      },
      "Success": {
        "type": "object",
        "properties": {
          "success": {
            "type": "boolean",
            "example": true
          },
          "data": {
            "type": "object"
          }
        }
      },
      "Schema": {
        "type": "object",
        "properties": {
          "name": {
            "type": "string"
          },
          "strict": {
            "type": "boolean"
          },
          "fields": {
            "type": "array",
            "items": {
              "$ref": "#/components/schemas/SchemaField"
            }
          }
        }
      },
      "SchemaField": {
        "type": "object",
        "properties": {
          "name": {
            "type": "string"
          },
          "type": {
            "type": "string",
            "enum": ["string", "number", "integer", "boolean", "array", "object"]
          },
          "required": {
            "type": "boolean"
          },
          "unique": {
            "type": "boolean"
          }
        }
      },
      "Document": {
        "type": "object",
        "properties": {
          "_id": {
            "type": "string",
            "description": "Unique document identifier"
          },
          "createdAt": {
            "type": "string",
            "format": "date-time"
          },
          "updatedAt": {
            "type": "string",
            "format": "date-time"
          }
        },
        "additionalProperties": true
      },
      "Collection": {
        "type": "object",
        "properties": {
          "name": {
            "type": "string"
          },
          "hasSchema": {
            "type": "boolean"
          },
          "strict": {
            "type": "boolean"
          },
          "count": {
            "type": "integer"
          }
        }
      },
      "Index": {
        "type": "object",
        "properties": {
          "field": {
            "type": "string"
          },
          "type": {
            "type": "string",
            "enum": ["btree", "hash"]
          },
          "name": {
            "type": "string"
          },
          "entryCount": {
            "type": "integer"
          }
        }
      },
      "AggregationPipeline": {
        "type": "array",
        "items": {
          "type": "object",
          "description": "Aggregation pipeline stage"
        },
        "example": [
          {
            "$match": {
              "status": "active"
            }
          },
          {
            "$group": {
              "_id": "$category",
              "total": {
                "$sum": "$amount"
              }
            }
          }
        ]
      },
      "MatchStage": {
        "type": "object",
        "description": "Filters documents to pass only those that match the specified condition(s).",
        "properties": {
          "$match": {
            "type": "object",
            "description": "Query conditions",
            "additionalProperties": true,
            "example": {
              "status": "active",
              "age": {
                "$gte": 18
              }
            }
          }
        }
      },
      "GroupStage": {
        "type": "object",
        "description": "Groups documents by a specified expression and applies accumulator expressions.",
        "properties": {
          "$group": {
            "type": "object",
            "required": ["_id"],
            "properties": {
              "_id": {
                "description": "Grouping key. Use null to group all documents, or a field reference with $ prefix.",
                "oneOf": [
                  {"type": "null"},
                  {"type": "string"},
                  {"type": "object"}
                ]
              }
            },
            "additionalProperties": {
              "type": "object",
              "description": "Accumulator expression",
              "example": {
                "$sum": "$amount"
              }
            }
          }
        }
      },
      "SortStage": {
        "type": "object",
        "description": "Sorts documents. Use 1 for ascending, -1 for descending.",
        "properties": {
          "$sort": {
            "type": "object",
            "additionalProperties": {
              "type": "integer",
              "enum": [1, -1]
            },
            "example": {
              "name": 1,
              "createdAt": -1
            }
          }
        }
      },
      "ProjectStage": {
        "type": "object",
        "description": "Reshapes each document. Include fields with true, exclude with false, or compute new fields.",
        "properties": {
          "$project": {
            "type": "object",
            "additionalProperties": {
              "oneOf": [
                {"type": "boolean"},
                {"type": "object"}
              ]
            },
            "example": {
              "name": 1,
              "email": 1,
              "fullName": {
                "$concat": ["$firstName", " ", "$lastName"]
              }
            }
          }
        }
      },
      "LimitStage": {
        "type": "object",
        "description": "Limits the number of documents passed to the next stage.",
        "properties": {
          "$limit": {
            "type": "integer",
            "example": 10
          }
        }
      },
      "SkipStage": {
        "type": "object",
        "description": "Skips the specified number of documents.",
        "properties": {
          "$skip": {
            "type": "integer",
            "example": 5
          }
        }
      },
      "LookupStage": {
        "type": "object",
        "description": "Performs a left outer join to another collection.",
        "properties": {
          "$lookup": {
            "type": "object",
            "required": ["from", "localField", "foreignField", "as"],
            "properties": {
              "from": {
                "type": "string",
                "description": "Collection to join"
              },
              "localField": {
                "type": "string",
                "description": "Field from the input documents"
              },
              "foreignField": {
                "type": "string",
                "description": "Field from the foreign collection"
              },
              "as": {
                "type": "string",
                "description": "Output array field name"
              }
            }
          }
        }
      },
      "UnwindStage": {
        "type": "object",
        "description": "Deconstructs an array field to output a document for each element.",
        "properties": {
          "$unwind": {
            "oneOf": [
              {"type": "string"},
              {
                "type": "object",
                "properties": {
                  "path": {"type": "string"},
                  "includeArrayIndex": {"type": "string"},
                  "preserveNullAndEmptyArrays": {"type": "boolean"}
                }
              }
            ]
          }
        }
      },
      "CountStage": {
        "type": "object",
        "description": "Returns a count of documents.",
        "properties": {
          "$count": {
            "type": "string",
            "description": "Name of the output field",
            "example": "total"
          }
        }
      },
      "AddFieldsStage": {
        "type": "object",
        "description": "Adds new fields to documents.",
        "properties": {
          "$addFields": {
            "type": "object",
            "description": "Field definitions",
            "additionalProperties": true
          }
        }
      },
      "BucketStage": {
        "type": "object",
        "description": "Categorizes documents into groups called buckets by a specified expression and bucket boundaries.",
        "properties": {
          "$bucket": {
            "type": "object",
            "required": ["groupBy", "boundaries"],
            "properties": {
              "groupBy": {
                "description": "Expression to group by"
              },
              "boundaries": {
                "type": "array",
                "description": "Array of values for bucket boundaries"
              },
              "default": {
                "description": "Bucket ID for documents that don't fit any bucket"
              },
              "output": {
                "type": "object",
                "description": "Output document fields"
              }
            }
          }
        }
      },
      "FacetStage": {
        "type": "object",
        "description": "Processes multiple aggregation pipelines within a single stage.",
        "properties": {
          "$facet": {
            "type": "object",
            "description": "Named pipelines to execute",
            "additionalProperties": {
              "type": "array",
              "items": {
                "type": "object"
              }
            }
          }
        }
      },
      "SortByCountStage": {
        "type": "object",
        "description": "Groups documents by a field and sorts by count descending.",
        "properties": {
          "$sortByCount": {
            "type": "string",
            "description": "Field to group by (with $ prefix)",
            "example": "$category"
          }
        }
      },
      "User": {
        "type": "object",
        "properties": {
          "id": {"type": "string"},
          "username": {"type": "string"},
          "email": {"type": "string"},
          "tenantId": {"type": "string"},
          "roles": {
            "type": "array",
            "items": {"type": "string"}
          },
          "createdAt": {"type": "string", "format": "date-time"}
        }
      },
      "APIKey": {
        "type": "object",
        "properties": {
          "id": {"type": "string"},
          "name": {"type": "string"},
          "key": {"type": "string", "description": "Only shown once at creation"},
          "createdAt": {"type": "string", "format": "date-time"},
          "expiresAt": {"type": "string", "format": "date-time"}
        }
      },
      "Role": {
        "type": "object",
        "properties": {
          "name": {"type": "string"},
          "description": {"type": "string"},
          "policies": {
            "type": "array",
            "items": {
              "$ref": "#/components/schemas/Policy"
            }
          }
        }
      },
      "Policy": {
        "type": "object",
        "properties": {
          "resource": {"type": "string"},
          "actions": {
            "type": "array",
            "items": {"type": "string"}
          },
          "fields": {
            "type": "array",
            "items": {"type": "string"}
          },
          "effect": {"type": "string", "enum": ["allow", "deny"]}
        }
      },
      "Tenant": {
        "type": "object",
        "properties": {
          "id": {"type": "string"},
          "name": {"type": "string"},
          "parentId": {"type": "string"},
          "createdAt": {"type": "string", "format": "date-time"}
        }
      },
      "Region": {
        "type": "object",
        "properties": {
          "id": {"type": "string"},
          "name": {"type": "string"},
          "tenantId": {"type": "string"},
          "createdAt": {"type": "string", "format": "date-time"}
        }
      },
      "Environment": {
        "type": "object",
        "properties": {
          "id": {"type": "string"},
          "name": {"type": "string"},
          "regionId": {"type": "string"},
          "createdAt": {"type": "string", "format": "date-time"}
        }
      }
    }
  },
  "paths": {
    "/health": {
      "get": {
        "tags": ["System"],
        "summary": "Health check",
        "description": "Returns the health status of the API server",
        "security": [],
        "responses": {
          "200": {
            "description": "Server is healthy",
            "content": {
              "application/json": {
                "schema": {
                  "type": "object",
                  "properties": {
                    "success": {"type": "boolean"},
                    "data": {
                      "type": "object",
                      "properties": {
                        "status": {"type": "string", "example": "healthy"}
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    },
    "/login": {
      "post": {
        "tags": ["Authentication"],
        "summary": "User login",
        "description": "Authenticate a user and receive a JWT token",
        "security": [],
        "requestBody": {
          "required": true,
          "content": {
            "application/json": {
              "schema": {
                "type": "object",
                "required": ["username", "password"],
                "properties": {
                  "username": {"type": "string"},
                  "password": {"type": "string", "format": "password"}
                }
              }
            }
          }
        },
        "responses": {
          "200": {
            "description": "Login successful",
            "content": {
              "application/json": {
                "schema": {
                  "type": "object",
                  "properties": {
                    "success": {"type": "boolean"},
                    "data": {
                      "type": "object",
                      "properties": {
                        "token": {"type": "string"},
                        "user": {"$ref": "#/components/schemas/User"}
                      }
                    }
                  }
                }
              }
            }
          },
          "401": {
            "description": "Invalid credentials",
            "content": {
              "application/json": {
                "schema": {"$ref": "#/components/schemas/Error"}
              }
            }
          }
        }
      }
    },
    "/register": {
      "post": {
        "tags": ["Authentication"],
        "summary": "Register a new user",
        "description": "Create a new user account",
        "security": [],
        "requestBody": {
          "required": true,
          "content": {
            "application/json": {
              "schema": {
                "type": "object",
                "required": ["username", "password"],
                "properties": {
                  "username": {"type": "string"},
                  "password": {"type": "string", "format": "password"},
                  "email": {"type": "string", "format": "email"},
                  "tenantId": {"type": "string"}
                }
              }
            }
          }
        },
        "responses": {
          "200": {
            "description": "User created",
            "content": {
              "application/json": {
                "schema": {
                  "type": "object",
                  "properties": {
                    "success": {"type": "boolean"},
                    "data": {"$ref": "#/components/schemas/User"}
                  }
                }
              }
            }
          },
          "400": {
            "description": "Invalid request",
            "content": {
              "application/json": {
                "schema": {"$ref": "#/components/schemas/Error"}
              }
            }
          }
        }
      }
    },
    "/collections": {
      "get": {
        "tags": ["Collections"],
        "summary": "List all collections",
        "description": "Returns a list of all collections in the database",
        "responses": {
          "200": {
            "description": "List of collections",
            "content": {
              "application/json": {
                "schema": {
                  "type": "object",
                  "properties": {
                    "success": {"type": "boolean"},
                    "data": {
                      "type": "object",
                      "properties": {
                        "collections": {
                          "type": "array",
                          "items": {"type": "string"}
                        },
                        "count": {"type": "integer"}
                      }
                    }
                  }
                }
              }
            }
          }
        }
      },
      "post": {
        "tags": ["Collections"],
        "summary": "Create a new collection",
        "description": "Creates a new collection with an optional schema",
        "requestBody": {
          "required": true,
          "content": {
            "application/json": {
              "schema": {
                "type": "object",
                "required": ["name"],
                "properties": {
                  "name": {"type": "string"},
                  "strict": {"type": "boolean", "default": false},
                  "schema": {"$ref": "#/components/schemas/Schema"}
                }
              },
              "examples": {
                "simple": {
                  "value": {
                    "name": "users"
                  }
                },
                "with_schema": {
                  "value": {
                    "name": "products",
                    "strict": true,
                    "schema": {
                      "name": "products",
                      "strict": true,
                      "fields": [
                        {"name": "name", "type": "string", "required": true},
                        {"name": "price", "type": "number", "required": true},
                        {"name": "category", "type": "string"}
                      ]
                    }
                  }
                }
              }
            }
          }
        },
        "responses": {
          "201": {
            "description": "Collection created",
            "content": {
              "application/json": {
                "schema": {
                  "type": "object",
                  "properties": {
                    "success": {"type": "boolean"},
                    "data": {"$ref": "#/components/schemas/Collection"}
                  }
                }
              }
            }
          },
          "409": {
            "description": "Collection already exists",
            "content": {
              "application/json": {
                "schema": {"$ref": "#/components/schemas/Error"}
              }
            }
          }
        }
      }
    },
    "/collections/{name}": {
      "get": {
        "tags": ["Collections"],
        "summary": "Get collection info",
        "description": "Returns information about a specific collection",
        "parameters": [
          {
            "name": "name",
            "in": "path",
            "required": true,
            "schema": {"type": "string"},
            "description": "Collection name"
          }
        ],
        "responses": {
          "200": {
            "description": "Collection info",
            "content": {
              "application/json": {
                "schema": {
                  "type": "object",
                  "properties": {
                    "success": {"type": "boolean"},
                    "data": {"$ref": "#/components/schemas/Collection"}
                  }
                }
              }
            }
          },
          "404": {
            "description": "Collection not found",
            "content": {
              "application/json": {
                "schema": {"$ref": "#/components/schemas/Error"}
              }
            }
          }
        }
      },
      "delete": {
        "tags": ["Collections"],
        "summary": "Delete a collection",
        "description": "Deletes a collection and all its documents",
        "parameters": [
          {
            "name": "name",
            "in": "path",
            "required": true,
            "schema": {"type": "string"}
          }
        ],
        "responses": {
          "200": {
            "description": "Collection deleted",
            "content": {
              "application/json": {
                "schema": {"$ref": "#/components/schemas/Success"}
              }
            }
          },
          "404": {
            "description": "Collection not found",
            "content": {
              "application/json": {
                "schema": {"$ref": "#/components/schemas/Error"}
              }
            }
          }
        }
      }
    },
    "/collections/{name}/documents": {
      "get": {
        "tags": ["Documents"],
        "summary": "Find documents",
        "description": "Retrieves documents from a collection, optionally filtered",
        "parameters": [
          {
            "name": "name",
            "in": "path",
            "required": true,
            "schema": {"type": "string"}
          },
          {
            "name": "filter",
            "in": "query",
            "required": false,
            "schema": {"type": "string"},
            "description": "JSON-encoded filter object",
            "example": "{\"status\":\"active\"}"
          }
        ],
        "responses": {
          "200": {
            "description": "List of documents",
            "content": {
              "application/json": {
                "schema": {
                  "type": "object",
                  "properties": {
                    "success": {"type": "boolean"},
                    "data": {
                      "type": "object",
                      "properties": {
                        "documents": {
                          "type": "array",
                          "items": {"$ref": "#/components/schemas/Document"}
                        },
                        "count": {"type": "integer"}
                      }
                    }
                  }
                }
              }
            }
          }
        }
      },
      "post": {
        "tags": ["Documents"],
        "summary": "Insert a document",
        "description": "Inserts a new document into a collection",
        "parameters": [
          {
            "name": "name",
            "in": "path",
            "required": true,
            "schema": {"type": "string"}
          }
        ],
        "requestBody": {
          "required": true,
          "content": {
            "application/json": {
              "schema": {
                "type": "object",
                "properties": {
                  "_id": {"type": "string", "description": "Optional custom ID"},
                  "data": {
                    "type": "object",
                    "description": "Document data"
                  }
                }
              },
              "example": {
                "data": {
                  "name": "John Doe",
                  "email": "john@example.com",
                  "age": 30
                }
              }
            }
          }
        },
        "responses": {
          "201": {
            "description": "Document created",
            "content": {
              "application/json": {
                "schema": {
                  "type": "object",
                  "properties": {
                    "success": {"type": "boolean"},
                    "data": {"$ref": "#/components/schemas/Document"}
                  }
                }
              }
            }
          },
          "400": {
            "description": "Validation error",
            "content": {
              "application/json": {
                "schema": {"$ref": "#/components/schemas/Error"}
              }
            }
          },
          "409": {
            "description": "Document already exists",
            "content": {
              "application/json": {
                "schema": {"$ref": "#/components/schemas/Error"}
              }
            }
          }
        }
      }
    },
    "/collections/{name}/documents/{id}": {
      "get": {
        "tags": ["Documents"],
        "summary": "Get a document",
        "description": "Retrieves a single document by ID",
        "parameters": [
          {
            "name": "name",
            "in": "path",
            "required": true,
            "schema": {"type": "string"}
          },
          {
            "name": "id",
            "in": "path",
            "required": true,
            "schema": {"type": "string"}
          }
        ],
        "responses": {
          "200": {
            "description": "Document found",
            "content": {
              "application/json": {
                "schema": {
                  "type": "object",
                  "properties": {
                    "success": {"type": "boolean"},
                    "data": {"$ref": "#/components/schemas/Document"}
                  }
                }
              }
            }
          },
          "404": {
            "description": "Document not found",
            "content": {
              "application/json": {
                "schema": {"$ref": "#/components/schemas/Error"}
              }
            }
          }
        }
      },
      "put": {
        "tags": ["Documents"],
        "summary": "Update a document",
        "description": "Replaces an entire document",
        "parameters": [
          {
            "name": "name",
            "in": "path",
            "required": true,
            "schema": {"type": "string"}
          },
          {
            "name": "id",
            "in": "path",
            "required": true,
            "schema": {"type": "string"}
          }
        ],
        "requestBody": {
          "required": true,
          "content": {
            "application/json": {
              "schema": {
                "type": "object",
                "properties": {
                  "data": {
                    "type": "object",
                    "description": "New document data"
                  }
                }
              }
            }
          }
        },
        "responses": {
          "200": {
            "description": "Document updated",
            "content": {
              "application/json": {
                "schema": {
                  "type": "object",
                  "properties": {
                    "success": {"type": "boolean"},
                    "data": {"$ref": "#/components/schemas/Document"}
                  }
                }
              }
            }
          },
          "404": {
            "description": "Document not found",
            "content": {
              "application/json": {
                "schema": {"$ref": "#/components/schemas/Error"}
              }
            }
          }
        }
      },
      "patch": {
        "tags": ["Documents"],
        "summary": "Patch a document",
        "description": "Partially updates a document",
        "parameters": [
          {
            "name": "name",
            "in": "path",
            "required": true,
            "schema": {"type": "string"}
          },
          {
            "name": "id",
            "in": "path",
            "required": true,
            "schema": {"type": "string"}
          }
        ],
        "requestBody": {
          "required": true,
          "content": {
            "application/json": {
              "schema": {
                "type": "object",
                "properties": {
                  "data": {
                    "type": "object",
                    "description": "Fields to update"
                  }
                }
              },
              "example": {
                "data": {
                  "age": 31
                }
              }
            }
          }
        },
        "responses": {
          "200": {
            "description": "Document patched",
            "content": {
              "application/json": {
                "schema": {
                  "type": "object",
                  "properties": {
                    "success": {"type": "boolean"},
                    "data": {"$ref": "#/components/schemas/Document"}
                  }
                }
              }
            }
          }
        }
      },
      "delete": {
        "tags": ["Documents"],
        "summary": "Delete a document",
        "description": "Deletes a document by ID",
        "parameters": [
          {
            "name": "name",
            "in": "path",
            "required": true,
            "schema": {"type": "string"}
          },
          {
            "name": "id",
            "in": "path",
            "required": true,
            "schema": {"type": "string"}
          }
        ],
        "responses": {
          "200": {
            "description": "Document deleted",
            "content": {
              "application/json": {
                "schema": {"$ref": "#/components/schemas/Success"}
              }
            }
          },
          "404": {
            "description": "Document not found",
            "content": {
              "application/json": {
                "schema": {"$ref": "#/components/schemas/Error"}
              }
            }
          }
        }
      }
    },
    "/collections/{name}/aggregate": {
      "post": {
        "tags": ["Aggregation"],
        "summary": "Run aggregation pipeline",
        "description": "Executes a MongoDB-like aggregation pipeline on a collection. Supports $match, $group, $sort, $project, $limit, $skip, $lookup, $unwind, $count, $facet, $bucket, $sortByCount, $addFields, $unset, $replaceRoot, and $set stages.",
        "parameters": [
          {
            "name": "name",
            "in": "path",
            "required": true,
            "schema": {"type": "string"}
          }
        ],
        "requestBody": {
          "required": true,
          "content": {
            "application/json": {
              "schema": {
                "type": "object",
                "required": ["pipeline"],
                "properties": {
                  "pipeline": {
                    "type": "array",
                    "items": {
                      "type": "object"
                    }
                  }
                }
              },
              "examples": {
                "simple_match": {
                  "summary": "Simple $match filter",
                  "value": {
                    "pipeline": [
                      {
                        "$match": {
                          "status": "active"
                        }
                      }
                    ]
                  }
                },
                "group_and_sort": {
                  "summary": "Group and sort results",
                  "value": {
                    "pipeline": [
                      {
                        "$match": {
                          "status": "active"
                        }
                      },
                      {
                        "$group": {
                          "_id": "$category",
                          "totalAmount": {
                            "$sum": "$amount"
                          },
                          "avgAmount": {
                            "$avg": "$amount"
                          },
                          "count": {
                            "$sum": 1
                          }
                        }
                      },
                      {
                        "$sort": {
                          "totalAmount": -1
                        }
                      }
                    ]
                  }
                },
                "lookup_join": {
                  "summary": "Join with another collection",
                  "value": {
                    "pipeline": [
                      {
                        "$lookup": {
                          "from": "orders",
                          "localField": "_id",
                          "foreignField": "userId",
                          "as": "orders"
                        }
                      },
                      {
                        "$unwind": "$orders"
                      }
                    ]
                  }
                },
                "facet_multiple": {
                  "summary": "Multiple aggregations with $facet",
                  "value": {
                    "pipeline": [
                      {
                        "$facet": {
                          "byCategory": [
                            {
                              "$group": {
                                "_id": "$category",
                                "count": {"$sum": 1}
                              }
                            }
                          ],
                          "byStatus": [
                            {
                              "$group": {
                                "_id": "$status",
                                "count": {"$sum": 1}
                              }
                            }
                          ],
                          "topProducts": [
                            {
                              "$sort": {"sales": -1}
                            },
                            {
                              "$limit": 5
                            }
                          ]
                        }
                      }
                    ]
                  }
                },
                "bucket_analysis": {
                  "summary": "Bucket analysis",
                  "value": {
                    "pipeline": [
                      {
                        "$bucket": {
                          "groupBy": "$price",
                          "boundaries": [0, 100, 500, 1000, 5000],
                          "default": "expensive",
                          "output": {
                            "count": {"$sum": 1},
                            "totalSales": {"$sum": "$sales"}
                          }
                        }
                      }
                    ]
                  }
                },
                "project_and_addfields": {
                  "summary": "Project and add computed fields",
                  "value": {
                    "pipeline": [
                      {
                        "$project": {
                          "name": 1,
                          "email": 1,
                          "fullName": {
                            "$concat": ["$firstName", " ", "$lastName"]
                          }
                        }
                      },
                      {
                        "$addFields": {
                          "domain": {
                            "$substr": ["$email", {"$add": [{"$indexOfBytes": ["$email", "@"]}, 1]}, 100]
                          }
                        }
                      }
                    ]
                  }
                }
              }
            }
          }
        },
        "responses": {
          "200": {
            "description": "Aggregation results",
            "content": {
              "application/json": {
                "schema": {
                  "type": "object",
                  "properties": {
                    "success": {"type": "boolean"},
                    "data": {
                      "type": "object",
                      "properties": {
                        "results": {
                          "type": "array",
                          "items": {
                            "type": "object"
                          }
                        },
                        "count": {"type": "integer"}
                      }
                    }
                  }
                }
              }
            }
          },
          "400": {
            "description": "Invalid pipeline",
            "content": {
              "application/json": {
                "schema": {"$ref": "#/components/schemas/Error"}
              }
            }
          }
        }
      }
    },
    "/collections/{name}/distinct/{field}": {
      "get": {
        "tags": ["Aggregation"],
        "summary": "Get distinct values",
        "description": "Returns distinct values for a field in a collection",
        "parameters": [
          {
            "name": "name",
            "in": "path",
            "required": true,
            "schema": {"type": "string"}
          },
          {
            "name": "field",
            "in": "path",
            "required": true,
            "schema": {"type": "string"}
          }
        ],
        "responses": {
          "200": {
            "description": "Distinct values",
            "content": {
              "application/json": {
                "schema": {
                  "type": "object",
                  "properties": {
                    "success": {"type": "boolean"},
                    "data": {
                      "type": "object",
                      "properties": {
                        "values": {
                          "type": "array",
                          "items": {}
                        },
                        "count": {"type": "integer"}
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    },
    "/collections/{name}/stats": {
      "get": {
        "tags": ["Collections"],
        "summary": "Get collection statistics",
        "description": "Returns statistics about a collection including document count and index information",
        "parameters": [
          {
            "name": "name",
            "in": "path",
            "required": true,
            "schema": {"type": "string"}
          }
        ],
        "responses": {
          "200": {
            "description": "Collection statistics",
            "content": {
              "application/json": {
                "schema": {
                  "type": "object",
                  "properties": {
                    "success": {"type": "boolean"},
                    "data": {
                      "type": "object",
                      "properties": {
                        "collection": {"type": "string"},
                        "count": {"type": "integer"},
                        "hasSchema": {"type": "boolean"},
                        "indexes": {
                          "type": "array",
                          "items": {"$ref": "#/components/schemas/Index"}
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    },
    "/collections/{name}/indexes": {
      "get": {
        "tags": ["Indexes"],
        "summary": "List indexes",
        "description": "Returns all indexes on a collection",
        "parameters": [
          {
            "name": "name",
            "in": "path",
            "required": true,
            "schema": {"type": "string"}
          }
        ],
        "responses": {
          "200": {
            "description": "List of indexes",
            "content": {
              "application/json": {
                "schema": {
                  "type": "object",
                  "properties": {
                    "success": {"type": "boolean"},
                    "data": {
                      "type": "object",
                      "properties": {
                        "collection": {"type": "string"},
                        "indexes": {
                          "type": "array",
                          "items": {"$ref": "#/components/schemas/Index"}
                        },
                        "count": {"type": "integer"}
                      }
                    }
                  }
                }
              }
            }
          }
        }
      },
      "post": {
        "tags": ["Indexes"],
        "summary": "Create an index",
        "description": "Creates an index on a field for faster queries. Use 'btree' for range queries and 'hash' for exact match queries.",
        "parameters": [
          {
            "name": "name",
            "in": "path",
            "required": true,
            "schema": {"type": "string"}
          }
        ],
        "requestBody": {
          "required": true,
          "content": {
            "application/json": {
              "schema": {
                "type": "object",
                "required": ["field"],
                "properties": {
                  "field": {"type": "string"},
                  "type": {
                    "type": "string",
                    "enum": ["btree", "hash"],
                    "default": "btree"
                  }
                }
              },
              "example": {
                "field": "email",
                "type": "hash"
              }
            }
          }
        },
        "responses": {
          "200": {
            "description": "Index created",
            "content": {
              "application/json": {
                "schema": {"$ref": "#/components/schemas/Success"}
              }
            }
          },
          "409": {
            "description": "Index already exists",
            "content": {
              "application/json": {
                "schema": {"$ref": "#/components/schemas/Error"}
              }
            }
          }
        }
      }
    },
    "/collections/{name}/indexes/{field}": {
      "delete": {
        "tags": ["Indexes"],
        "summary": "Drop an index",
        "description": "Removes an index from a collection",
        "parameters": [
          {
            "name": "name",
            "in": "path",
            "required": true,
            "schema": {"type": "string"}
          },
          {
            "name": "field",
            "in": "path",
            "required": true,
            "schema": {"type": "string"}
          }
        ],
        "responses": {
          "200": {
            "description": "Index dropped",
            "content": {
              "application/json": {
                "schema": {"$ref": "#/components/schemas/Success"}
              }
            }
          },
          "404": {
            "description": "Index not found",
            "content": {
              "application/json": {
                "schema": {"$ref": "#/components/schemas/Error"}
              }
            }
          }
        }
      }
    },
    "/collections/{name}/schema": {
      "get": {
        "tags": ["Schema"],
        "summary": "Get collection schema",
        "description": "Returns the schema definition for a collection",
        "parameters": [
          {
            "name": "name",
            "in": "path",
            "required": true,
            "schema": {"type": "string"}
          }
        ],
        "responses": {
          "200": {
            "description": "Schema information",
            "content": {
              "application/json": {
                "schema": {
                  "type": "object",
                  "properties": {
                    "success": {"type": "boolean"},
                    "data": {
                      "type": "object",
                      "properties": {
                        "hasSchema": {"type": "boolean"},
                        "schema": {"$ref": "#/components/schemas/Schema"}
                      }
                    }
                  }
                }
              }
            }
          }
        }
      },
      "put": {
        "tags": ["Schema"],
        "summary": "Update collection schema",
        "description": "Sets or updates the schema for a collection",
        "parameters": [
          {
            "name": "name",
            "in": "path",
            "required": true,
            "schema": {"type": "string"}
          }
        ],
        "requestBody": {
          "required": true,
          "content": {
            "application/json": {
              "schema": {
                "type": "object",
                "properties": {
                  "schema": {"$ref": "#/components/schemas/Schema"}
                }
              }
            }
          }
        },
        "responses": {
          "200": {
            "description": "Schema updated",
            "content": {
              "application/json": {
                "schema": {"$ref": "#/components/schemas/Success"}
              }
            }
          }
        }
      }
    },
    "/collections/{name}/export": {
      "get": {
        "tags": ["Import/Export"],
        "summary": "Export collection",
        "description": "Exports a collection to JSON format",
        "parameters": [
          {
            "name": "name",
            "in": "path",
            "required": true,
            "schema": {"type": "string"}
          },
          {
            "name": "download",
            "in": "query",
            "required": false,
            "schema": {"type": "boolean"},
            "description": "Set to true to download as file"
          }
        ],
        "responses": {
          "200": {
            "description": "Exported collection data",
            "content": {
              "application/json": {
                "schema": {
                  "type": "object",
                  "properties": {
                    "name": {"type": "string"},
                    "hasSchema": {"type": "boolean"},
                    "schema": {"$ref": "#/components/schemas/Schema"},
                    "documents": {
                      "type": "array",
                      "items": {"$ref": "#/components/schemas/Document"}
                    },
                    "exportedAt": {"type": "string", "format": "date-time"}
                  }
                }
              }
            }
          }
        }
      }
    },
    "/collections/{name}/import": {
      "post": {
        "tags": ["Import/Export"],
        "summary": "Import collection",
        "description": "Imports a collection from JSON data",
        "parameters": [
          {
            "name": "name",
            "in": "path",
            "required": true,
            "schema": {"type": "string"}
          },
          {
            "name": "overwrite",
            "in": "query",
            "required": false,
            "schema": {"type": "boolean"},
            "description": "Set to true to overwrite existing collection"
          }
        ],
        "requestBody": {
          "required": true,
          "content": {
            "application/json": {
              "schema": {
                "type": "object",
                "properties": {
                  "name": {"type": "string"},
                  "hasSchema": {"type": "boolean"},
                  "schema": {"$ref": "#/components/schemas/Schema"},
                  "documents": {
                    "type": "array",
                    "items": {"$ref": "#/components/schemas/Document"}
                  }
                }
              }
            }
          }
        },
        "responses": {
          "200": {
            "description": "Import successful",
            "content": {
              "application/json": {
                "schema": {"$ref": "#/components/schemas/Success"}
              }
            }
          },
          "409": {
            "description": "Collection already exists",
            "content": {
              "application/json": {
                "schema": {"$ref": "#/components/schemas/Error"}
              }
            }
          }
        }
      }
    },
    "/apikeys": {
      "post": {
        "tags": ["Authentication"],
        "summary": "Create API key",
        "description": "Creates a new API key for the authenticated user",
        "requestBody": {
          "required": true,
          "content": {
            "application/json": {
              "schema": {
                "type": "object",
                "properties": {
                  "name": {"type": "string"},
                  "expiresIn": {
                    "type": "integer",
                    "description": "Expiration time in seconds"
                  }
                }
              }
            }
          }
        },
        "responses": {
          "200": {
            "description": "API key created",
            "content": {
              "application/json": {
                "schema": {
                  "type": "object",
                  "properties": {
                    "success": {"type": "boolean"},
                    "data": {
                      "type": "object",
                      "properties": {
                        "apiKey": {"$ref": "#/components/schemas/APIKey"},
                        "key": {"type": "string"}
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    },
    "/roles": {
      "post": {
        "tags": ["RBAC"],
        "summary": "Create a role",
        "description": "Creates a new role with specified policies",
        "requestBody": {
          "required": true,
          "content": {
            "application/json": {
              "schema": {
                "type": "object",
                "required": ["name"],
                "properties": {
                  "name": {"type": "string"},
                  "description": {"type": "string"},
                  "policies": {
                    "type": "array",
                    "items": {"$ref": "#/components/schemas/Policy"}
                  }
                }
              },
              "example": {
                "name": "editor",
                "description": "Can read and write documents",
                "policies": [
                  {
                    "resource": "documents",
                    "actions": ["read", "create", "update"],
                    "fields": ["*"],
                    "effect": "allow"
                  }
                ]
              }
            }
          }
        },
        "responses": {
          "200": {
            "description": "Role created",
            "content": {
              "application/json": {
                "schema": {"$ref": "#/components/schemas/Success"}
              }
            }
          }
        }
      }
    },
    "/users/roles": {
      "post": {
        "tags": ["RBAC"],
        "summary": "Assign role to user",
        "description": "Assigns a role to a user",
        "requestBody": {
          "required": true,
          "content": {
            "application/json": {
              "schema": {
                "type": "object",
                "required": ["userId", "roleId"],
                "properties": {
                  "userId": {"type": "string"},
                  "roleId": {"type": "string"}
                }
              }
            }
          }
        },
        "responses": {
          "200": {
            "description": "Role assigned",
            "content": {
              "application/json": {
                "schema": {"$ref": "#/components/schemas/Success"}
              }
            }
          }
        }
      }
    },
    "/tenants": {
      "get": {
        "tags": ["Hierarchy"],
        "summary": "List tenants",
        "description": "Returns all tenants",
        "responses": {
          "200": {
            "description": "List of tenants",
            "content": {
              "application/json": {
                "schema": {
                  "type": "object",
                  "properties": {
                    "success": {"type": "boolean"},
                    "data": {
                      "type": "array",
                      "items": {"$ref": "#/components/schemas/Tenant"}
                    }
                  }
                }
              }
            }
          }
        }
      },
      "post": {
        "tags": ["Hierarchy"],
        "summary": "Create tenant",
        "description": "Creates a new tenant",
        "requestBody": {
          "required": true,
          "content": {
            "application/json": {
              "schema": {
                "type": "object",
                "required": ["name"],
                "properties": {
                  "name": {"type": "string"},
                  "parentId": {"type": "string"}
                }
              }
            }
          }
        },
        "responses": {
          "200": {
            "description": "Tenant created",
            "content": {
              "application/json": {
                "schema": {
                  "type": "object",
                  "properties": {
                    "success": {"type": "boolean"},
                    "data": {"$ref": "#/components/schemas/Tenant"}
                  }
                }
              }
            }
          }
        }
      }
    },
    "/tenants/{id}": {
      "delete": {
        "tags": ["Hierarchy"],
        "summary": "Delete tenant",
        "parameters": [
          {
            "name": "id",
            "in": "path",
            "required": true,
            "schema": {"type": "string"}
          }
        ],
        "responses": {
          "200": {
            "description": "Tenant deleted",
            "content": {
              "application/json": {
                "schema": {"$ref": "#/components/schemas/Success"}
              }
            }
          }
        }
      }
    },
    "/regions": {
      "get": {
        "tags": ["Hierarchy"],
        "summary": "List regions",
        "responses": {
          "200": {
            "description": "List of regions",
            "content": {
              "application/json": {
                "schema": {
                  "type": "object",
                  "properties": {
                    "success": {"type": "boolean"},
                    "data": {
                      "type": "array",
                      "items": {"$ref": "#/components/schemas/Region"}
                    }
                  }
                }
              }
            }
          }
        }
      },
      "post": {
        "tags": ["Hierarchy"],
        "summary": "Create region",
        "requestBody": {
          "required": true,
          "content": {
            "application/json": {
              "schema": {
                "type": "object",
                "required": ["name", "tenantId"],
                "properties": {
                  "name": {"type": "string"},
                  "tenantId": {"type": "string"}
                }
              }
            }
          }
        },
        "responses": {
          "200": {
            "description": "Region created"
          }
        }
      }
    },
    "/regions/{id}": {
      "delete": {
        "tags": ["Hierarchy"],
        "summary": "Delete region",
        "parameters": [
          {
            "name": "id",
            "in": "path",
            "required": true,
            "schema": {"type": "string"}
          }
        ],
        "responses": {
          "200": {
            "description": "Region deleted"
          }
        }
      }
    },
    "/environments": {
      "get": {
        "tags": ["Hierarchy"],
        "summary": "List environments",
        "responses": {
          "200": {
            "description": "List of environments",
            "content": {
              "application/json": {
                "schema": {
                  "type": "object",
                  "properties": {
                    "success": {"type": "boolean"},
                    "data": {
                      "type": "array",
                      "items": {"$ref": "#/components/schemas/Environment"}
                    }
                  }
                }
              }
            }
          }
        }
      },
      "post": {
        "tags": ["Hierarchy"],
        "summary": "Create environment",
        "requestBody": {
          "required": true,
          "content": {
            "application/json": {
              "schema": {
                "type": "object",
                "required": ["name", "regionId"],
                "properties": {
                  "name": {"type": "string"},
                  "regionId": {"type": "string"}
                }
              }
            }
          }
        },
        "responses": {
          "200": {
            "description": "Environment created"
          }
        }
      }
    },
    "/environments/{id}": {
      "delete": {
        "tags": ["Hierarchy"],
        "summary": "Delete environment",
        "parameters": [
          {
            "name": "id",
            "in": "path",
            "required": true,
            "schema": {"type": "string"}
          }
        ],
        "responses": {
          "200": {
            "description": "Environment deleted"
          }
        }
      }
    }
  },
  "tags": [
    {
      "name": "System",
      "description": "System health and status"
    },
    {
      "name": "Authentication",
      "description": "User authentication and API key management"
    },
    {
      "name": "Collections",
      "description": "Collection management operations"
    },
    {
      "name": "Documents",
      "description": "Document CRUD operations"
    },
    {
      "name": "Aggregation",
      "description": "MongoDB-like aggregation pipeline operations"
    },
    {
      "name": "Indexes",
      "description": "Index management for query optimization"
    },
    {
      "name": "Schema",
      "description": "Schema definition and validation"
    },
    {
      "name": "Import/Export",
      "description": "Collection import and export operations"
    },
    {
      "name": "RBAC",
      "description": "Role-based access control management"
    },
    {
      "name": "Hierarchy",
      "description": "Multi-tenant hierarchy management"
    }
  ],
  "x-workflows": [
    {
      "name": "Basic CRUD Workflow",
      "description": "Create a collection, insert documents, and query them",
      "steps": [
        {
          "name": "Create collection",
          "method": "POST",
          "path": "/collections",
          "body": {
            "name": "products",
            "strict": true,
            "schema": {
              "name": "products",
              "strict": true,
              "fields": [
                {"name": "name", "type": "string", "required": true},
                {"name": "price", "type": "number", "required": true},
                {"name": "category", "type": "string"},
                {"name": "inStock", "type": "boolean"}
              ]
            }
          }
        },
        {
          "name": "Insert document",
          "method": "POST",
          "path": "/collections/products/documents",
          "body": {
            "data": {
              "name": "Laptop",
              "price": 999.99,
              "category": "electronics",
              "inStock": true
            }
          }
        },
        {
          "name": "Query documents",
          "method": "GET",
          "path": "/collections/products/documents",
          "queryParams": {
            "filter": "{\"category\":\"electronics\"}"
          }
        }
      ]
    },
    {
      "name": "Aggregation Analysis Workflow",
      "description": "Perform complex data analysis using aggregation pipelines",
      "steps": [
        {
          "name": "Create sales collection",
          "method": "POST",
          "path": "/collections",
          "body": {
            "name": "sales"
          }
        },
        {
          "name": "Insert sample data",
          "method": "POST",
          "path": "/collections/sales/documents",
          "body": {
            "data": [
              {"product": "Laptop", "amount": 999.99, "category": "electronics", "date": "2024-01-15"},
              {"product": "Mouse", "amount": 29.99, "category": "electronics", "date": "2024-01-16"},
              {"product": "Desk", "amount": 299.99, "category": "furniture", "date": "2024-01-17"}
            ]
          }
        },
        {
          "name": "Aggregate by category",
          "method": "POST",
          "path": "/collections/sales/aggregate",
          "body": {
            "pipeline": [
              {
                "$group": {
                  "_id": "$category",
                  "totalSales": {"$sum": "$amount"},
                  "avgSale": {"$avg": "$amount"},
                  "count": {"$sum": 1}
                }
              },
              {
                "$sort": {"totalSales": -1}
              }
            ]
          }
        }
      ]
    },
    {
      "name": "Index Optimization Workflow",
      "description": "Create indexes to optimize query performance",
      "steps": [
        {
          "name": "Create hash index for exact matches",
          "method": "POST",
          "path": "/collections/users/indexes",
          "body": {
            "field": "email",
            "type": "hash"
          }
        },
        {
          "name": "Create B-tree index for range queries",
          "method": "POST",
          "path": "/collections/orders/indexes",
          "body": {
            "field": "createdAt",
            "type": "btree"
          }
        },
        {
          "name": "View indexes",
          "method": "GET",
          "path": "/collections/users/indexes"
        }
      ]
    },
    {
      "name": "Cross-Collection Join Workflow",
      "description": "Use $lookup to join data across collections",
      "steps": [
        {
          "name": "Create users collection",
          "method": "POST",
          "path": "/collections",
          "body": {"name": "users"}
        },
        {
          "name": "Create orders collection",
          "method": "POST",
          "path": "/collections",
          "body": {"name": "orders"}
        },
        {
          "name": "Join orders with users",
          "method": "POST",
          "path": "/collections/users/aggregate",
          "body": {
            "pipeline": [
              {
                "$lookup": {
                  "from": "orders",
                  "localField": "_id",
                  "foreignField": "userId",
                  "as": "userOrders"
                }
              },
              {
                "$unwind": {
                  "path": "$userOrders",
                  "preserveNullAndEmptyArrays": true
                }
              }
            ]
          }
        }
      ]
    },
    {
      "name": "Multi-Tenant Setup Workflow",
      "description": "Set up a multi-tenant environment with RBAC",
      "steps": [
        {
          "name": "Create tenant",
          "method": "POST",
          "path": "/tenants",
          "body": {
            "name": "Acme Corp"
          }
        },
        {
          "name": "Create region",
          "method": "POST",
          "path": "/regions",
          "body": {
            "name": "US-East",
            "tenantId": "tenant_id_from_previous_step"
          }
        },
        {
          "name": "Create role with policies",
          "method": "POST",
          "path": "/roles",
          "body": {
            "name": "tenant_reader",
            "description": "Read-only access to tenant data",
            "policies": [
              {
                "resource": "documents",
                "actions": ["read"],
                "fields": ["*"],
                "effect": "allow"
              }
            ]
          }
        },
        {
          "name": "Register user",
          "method": "POST",
          "path": "/register",
          "body": {
            "username": "acme_user",
            "password": "secure_password",
            "tenantId": "tenant_id"
          }
        },
        {
          "name": "Assign role to user",
          "method": "POST",
          "path": "/users/roles",
          "body": {
            "userId": "user_id",
            "roleId": "tenant_reader"
          }
        }
      ]
    }
  ]
}`

// GetSwaggerJSON returns the swagger specification as JSON
func GetSwaggerJSON() ([]byte, error) {
	return json.Marshal(json.RawMessage(openAPISpec))
}