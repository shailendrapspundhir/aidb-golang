# AIDB - AI-Native Database

AIDB is an AI-native database built from scratch in Go, designed to store schema-full and schema-less JSON documents, with future support for vectors, text data, and embeddings.

## Current Features

- **JSON Document Storage**: Store, retrieve, update, and delete JSON documents
- **Schema-less Collections**: Create collections without predefined schemas
- **Schema-full Collections**: Define strict schemas with field types and validation
- **REST API**: Full CRUD operations via REST endpoints
- **BoltDB Persistence**: Data is automatically persisted using BoltDB and survives server restarts
- **Export/Import**: Export and import collections with or without schemas

## Project Structure

```
aidb-golang/
├── main.go                     # Entry point and HTTP server
├── go.mod
├── README.md
├── .env.example                # Environment configuration template
├── test_api.sh                 # API test script
├── internal/
│   ├── config/
│   │   └── config.go           # Configuration management
│   ├── document/
│   │   └── document.go         # Document types and schema validation
│   ├── storage/
│   │   ├── storage.go          # Storage interface
│   │   ├── memory.go           # In-memory storage implementation
│   │   ├── boltdb.go           # BoltDB persistent storage
│   │   └── persistent.go       # File-based persistent storage (deprecated)
│   ├── collection/
│   │   └── collection.go       # Collection management
│   └── api/
│       └── handlers.go         # REST API handlers
└── pkg/
    └── utils/
```

## Getting Started

### Prerequisites

- Go 1.22 or higher

### Installation

```bash
# Clone the repository
git clone <repository-url>
cd aidb-golang

# Download dependencies
go mod tidy

# (Optional) Copy and edit environment configuration
cp .env.example .env

# Run the server
go run main.go
```

The server will start on port 11111 and create a `./aidb_data/aidb.db` BoltDB file for persistent storage.

### Configuration

AIDB can be configured using environment variables:

| Variable | Description | Default |
|----------|-------------|---------|
| `AIDB_DATA_DIR` | Directory for exports/imports | `./aidb_data` |
| `AIDB_DATABASE_FILE` | BoltDB database file path | `./aidb_data/aidb.db` |
| `AIDB_SERVER_PORT` | Server port | `11111` |

You can also create a `.env` file by copying `.env.example`.

## API Documentation

### Base URL
```
http://localhost:11111/api/v1
```

### Collections

#### List all collections
```http
GET /api/v1/collections
```

Response:
```json
{
  "success": true,
  "data": {
    "collections": ["users", "products"],
    "count": 2
  }
}
```

#### Create a collection (schema-less)
```http
POST /api/v1/collections
Content-Type: application/json

{
  "name": "users"
}
```

#### Create a collection with schema
```http
POST /api/v1/collections
Content-Type: application/json

{
  "name": "products",
  "schema": {
    "name": "products",
    "strict": true,
    "fields": {
      "name": {
        "type": "string",
        "required": true
      },
      "price": {
        "type": "number",
        "required": true
      },
      "inStock": {
        "type": "boolean"
      }
    }
  }
}
```

#### Get collection info
```http
GET /api/v1/collections/{name}
```

#### Delete a collection
```http
DELETE /api/v1/collections/{name}
```

### Documents

#### Insert a document
```http
POST /api/v1/collections/{name}/documents
Content-Type: application/json

{
  "data": {
    "name": "John Doe",
    "email": "john@example.com",
    "age": 30
  }
}
```

Response:
```json
{
  "success": true,
  "data": {
    "_id": "550e8400-e29b-41d4-a716-446655440000",
    "_createdAt": "2024-01-15T10:30:00Z",
    "_updatedAt": "2024-01-15T10:30:00Z",
    "data": {
      "name": "John Doe",
      "email": "john@example.com",
      "age": 30
    }
  }
}
```

#### Insert with custom ID
```http
POST /api/v1/collections/{name}/documents
Content-Type: application/json

{
  "_id": "user-001",
  "data": {
    "name": "Jane Doe"
  }
}
```

#### Get a document by ID
```http
GET /api/v1/collections/{name}/documents/{id}
```

#### List all documents
```http
GET /api/v1/collections/{name}/documents
```

#### Find documents with filter
```http
GET /api/v1/collections/{name}/documents?name=John
```

Or with JSON filter:
```http
GET /api/v1/collections/{name}/documents?filter={"age":30}
```

#### Update a document (full replace)
```http
PUT /api/v1/collections/{name}/documents/{id}
Content-Type: application/json

{
  "data": {
    "name": "John Smith",
    "email": "johnsmith@example.com"
  }
}
```

#### Patch a document (partial update)
```http
PATCH /api/v1/collections/{name}/documents/{id}
Content-Type: application/json

{
  "data": {
    "email": "newemail@example.com"
  }
}
```

#### Delete a document
```http
DELETE /api/v1/collections/{name}/documents/{id}
```

### Schema

#### Get collection schema
```http
GET /api/v1/collections/{name}/schema
```

#### Set/Update collection schema
```http
PUT /api/v1/collections/{name}/schema
Content-Type: application/json

{
  "schema": {
    "name": "users",
    "strict": false,
    "fields": {
      "email": {
        "type": "string",
        "required": true
      }
    }
  }
}
```

### Health Check
```http
GET /api/v1/health
```

### Export/Import

#### Export a collection
```http
GET /api/v1/collections/{name}/export
```

Response:
```json
{
  "success": true,
  "data": {
    "name": "users",
    "hasSchema": false,
    "schema": null,
    "documents": [
      {
        "_id": "abc123",
        "_createdAt": "2024-01-15T10:30:00Z",
        "_updatedAt": "2024-01-15T10:30:00Z",
        "data": {
          "name": "John Doe",
          "email": "john@example.com"
        }
      }
    ],
    "exportedAt": "2024-01-15T12:00:00Z"
  }
}
```

#### Export as downloadable file
```http
GET /api/v1/collections/{name}/export?download=true
```

#### Import a collection
```http
POST /api/v1/collections/{name}/import
Content-Type: application/json

{
  "name": "imported_users",
  "hasSchema": false,
  "documents": [
    {
      "_id": "user-1",
      "_createdAt": "2024-01-01T00:00:00Z",
      "_updatedAt": "2024-01-01T00:00:00Z",
      "data": {
        "name": "Imported User",
        "email": "imported@example.com"
      }
    }
  ]
}
```

#### Import with schema (schema-full)
```http
POST /api/v1/collections/{name}/import
Content-Type: application/json

{
  "name": "imported_products",
  "hasSchema": true,
  "schema": {
    "name": "products",
    "strict": true,
    "fields": {
      "name": {"type": "string", "required": true},
      "price": {"type": "number", "required": true}
    }
  },
  "documents": [
    {
      "_id": "product-1",
      "_createdAt": "2024-01-01T00:00:00Z",
      "_updatedAt": "2024-01-01T00:00:00Z",
      "data": {
        "name": "Widget",
        "price": 19.99
      }
    }
  ]
}
```

#### Import with overwrite
```http
POST /api/v1/collections/{name}/import?overwrite=true
Content-Type: application/json

{
  "name": "existing_collection",
  "hasSchema": false,
  "documents": [...]
}
```

## Schema Field Types

| Type | Description |
|------|-------------|
| `string` | String value |
| `number` | Float or integer |
| `integer` | Integer value |
| `boolean` | Boolean value |
| `array` | JSON array |
| `object` | JSON object |
| `null` | Null value |
| `any` | Any type |

## Schema Field Options

| Option | Type | Description |
|--------|------|-------------|
| `type` | string | Field type (required) |
| `required` | boolean | Field must be present |
| `default` | any | Default value if not provided |
| `description` | string | Field description |
| `enum` | array | Allowed values |
| `minLength` | int | Minimum string length |
| `maxLength` | int | Maximum string length |
| `minimum` | number | Minimum numeric value |
| `maximum` | number | Maximum numeric value |

## Example Usage

### Creating a schema-less collection and adding documents

```bash
# Create a schema-less collection
curl -X POST http://localhost:11111/api/v1/collections \
  -H "Content-Type: application/json" \
  -d '{"name": "blog_posts"}'

# Add a document
curl -X POST http://localhost:11111/api/v1/collections/blog_posts/documents \
  -H "Content-Type: application/json" \
  -d '{
    "data": {
      "title": "My First Post",
      "content": "Hello World!",
      "tags": ["intro", "first"],
      "views": 0
    }
  }'

# Get all documents
curl http://localhost:11111/api/v1/collections/blog_posts/documents
```

### Creating a schema-full collection

```bash
# Create a collection with strict schema
curl -X POST http://localhost:11111/api/v1/collections \
  -H "Content-Type: application/json" \
  -d '{
    "name": "products",
    "schema": {
      "name": "products",
      "strict": true,
      "fields": {
        "name": {
          "type": "string",
          "required": true,
          "minLength": 1,
          "maxLength": 100
        },
        "price": {
          "type": "number",
          "required": true,
          "minimum": 0
        },
        "category": {
          "type": "string",
          "required": true,
          "enum": ["electronics", "books", "clothing"]
        },
        "inStock": {
          "type": "boolean",
          "default": true
        }
      }
    }
  }'

# This will fail validation (missing required field)
curl -X POST http://localhost:11111/api/v1/collections/products/documents \
  -H "Content-Type: application/json" \
  -d '{"data": {"name": "Widget"}}'

# This will succeed
curl -X POST http://localhost:11111/api/v1/collections/products/documents \
  -H "Content-Type: application/json" \
  -d '{
    "data": {
      "name": "Smartphone",
      "price": 699.99,
      "category": "electronics"
    }
  }'
```

## Testing

A comprehensive test script is provided to test all API endpoints:

```bash
# Make sure the server is running
go run main.go &

# Run the test script
./test_api.sh
```

The test script covers:
- Health check endpoint
- Collection CRUD operations (schema-less and schema-full)
- Document CRUD operations
- Schema validation (required fields, types, enums, strict mode)
- Filter queries
- Export/Import operations
- Error handling

### Testing Persistence

To verify that data persists across server restarts:

```bash
# 1. Start the server and run tests
go run main.go &
./test_api.sh

# 2. Stop the server (Ctrl+C)

# 3. Restart the server
go run main.go &

# 4. Check that data still exists
curl http://localhost:11111/api/v1/collections
```

Data is stored in `./aidb_data/aidb.db` (BoltDB file) by default.

## Roadmap

- [x] JSON Document Storage
- [x] Schema-less Collections
- [x] Schema-full Collections with Validation
- [x] REST API
- [x] Persistent Storage (BoltDB)
- [x] Export/Import Collections
- [ ] Vector Storage and Search
- [ ] Text Search with Indexing
- [ ] Embedding Generation and Storage
- [ ] Query Language (AQL - AIDB Query Language)
- [ ] Transactions
- [ ] Replication
- [ ] Clustering

## License

MIT
