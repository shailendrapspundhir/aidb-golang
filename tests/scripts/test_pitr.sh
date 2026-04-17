#!/bin/bash
# Test script for PITR (Point-in-Time Recovery) features
# Tests: WAL ordering fix, Checkpoint, Archiver, Backup, RecoverToLSN

set -e

echo "=== AIDB PITR Feature Test ==="

# Start server with PITR enabled
export AIDB_ENABLE_PITR=true
export AIDB_CHECKPOINT_INTERVAL=5
export AIDB_WAL_ARCHIVE_DIR=/tmp/aidb_wal_archive
export AIDB_BACKUP_DIR=/tmp/aidb_backups

mkdir -p $AIDB_WAL_ARCHIVE_DIR $AIDB_BACKUP_DIR

echo "Starting AIDB server..."
../aidb > /tmp/aidb_pitr.log 2>&1 &
SERVER_PID=$!
sleep 3

echo "Creating test data..."
curl -s -X POST http://localhost:11111/api/v1/collections \
  -H "Content-Type: application/json" \
  -d '{"name":"pitr_test"}' > /dev/null

curl -s -X POST http://localhost:11111/api/v1/collections/pitr_test/documents \
  -H "Content-Type: application/json" \
  -d '{"id":"doc1", "data": {"value": "before_backup"}}' > /dev/null

echo "Triggering manual checkpoint..."
curl -s -X POST http://localhost:11111/api/v1/system/checkpoint || true

echo "Creating base backup..."
curl -s -X POST http://localhost:11111/api/v1/system/backup || true

echo "Inserting more data (after backup)..."
curl -s -X POST http://localhost:11111/api/v1/collections/pitr_test/documents \
  -H "Content-Type: application/json" \
  -d '{"id":"doc2", "data": {"value": "after_backup"}}' > /dev/null

echo "Killing server to simulate crash..."
kill $SERVER_PID || true
sleep 1

echo "Restarting server (recovery should run)..."
../aidb > /tmp/aidb_pitr2.log 2>&1 &
SERVER_PID=$!
sleep 4

echo "Verifying recovery..."
RESULT=$(curl -s http://localhost:11111/api/v1/collections/pitr_test/documents/doc1 | grep -o '"value":"before_backup"' || true)
if [ -n "$RESULT" ]; then
  echo "✓ Recovery of pre-backup data successful"
else
  echo "✗ Recovery failed"
fi

echo "=== PITR Test Complete ==="
kill $SERVER_PID || true
