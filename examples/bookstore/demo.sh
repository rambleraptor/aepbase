#!/usr/bin/env bash
# Bookstore demo: uses aepcli for ALL operations.
#
# Prerequisites:
#   1. Build aepcli:  cd ../../aepcli && go build -o aepcli ./cmd/aepcli
#   2. Start server:  go run ./examples/bookstore
#
# Usage:
#   ./examples/bookstore/demo.sh [path-to-aepcli]

set -euo pipefail

AEPCLI="${1:-../../aepcli/aepcli}"
API="http://localhost:8080/openapi.json"

if ! command -v "$AEPCLI" &>/dev/null && [[ ! -x "$AEPCLI" ]]; then
  echo "aepcli not found at $AEPCLI"
  echo "Build it first: cd ../../aepcli && go build -o aepcli ./cmd/aepcli"
  exit 1
fi

echo "=== Bookstore Demo ==="
echo ""

# --- Step 1: Create resource definitions ---
echo "--- Creating resource definitions ---"

echo "Creating publisher resource..."
"$AEPCLI" "$API" resource create publisher \
  --singular publisher \
  --plural publishers \
  --schema '{"type":"object","properties":{"name":{"type":"string"},"location":{"type":"string"}}}'

echo ""
echo "Creating book resource (child of publisher)..."
"$AEPCLI" "$API" resource create book \
  --singular book \
  --plural books \
  --schema '{"type":"object","properties":{"title":{"type":"string"},"author":{"type":"string"},"published":{"type":"boolean"},"purchase_count":{"type":"integer"}}}' \
  --parents publisher

echo ""

# --- Step 2: Create publisher and book instances ---
echo "--- Creating resources ---"

echo "Creating publisher 'acme-books'..."
"$AEPCLI" "$API" publisher create acme-books \
  --name "Acme Books" \
  --location "New York"

echo ""
echo "Creating book 'great-gatsby'..."
"$AEPCLI" "$API" book --publisher acme-books create great-gatsby \
  --title "The Great Gatsby" \
  --author "F. Scott Fitzgerald" \
  --purchase_count 0

echo ""
echo "Creating book 'moby-dick'..."
"$AEPCLI" "$API" book --publisher acme-books create moby-dick \
  --title "Moby Dick" \
  --author "Herman Melville" \
  --purchase_count 0

echo ""

# --- Step 3: Verify resources were created ---
echo "--- Listing resources ---"

echo "Publishers:"
"$AEPCLI" "$API" publisher list
echo ""

echo "Books (under acme-books):"
"$AEPCLI" "$API" book --publisher acme-books list
echo ""

# --- Step 4: Call custom methods ---
echo "--- Calling custom methods ---"

echo "Publishing 'great-gatsby'..."
"$AEPCLI" "$API" book --publisher acme-books :publish great-gatsby
echo ""

echo "Purchasing 'great-gatsby' (quantity: 3)..."
"$AEPCLI" "$API" book --publisher acme-books :purchase great-gatsby \
  --quantity 3
echo ""

echo "Purchasing 'great-gatsby' again (quantity: 2)..."
"$AEPCLI" "$API" book --publisher acme-books :purchase great-gatsby \
  --quantity 2
echo ""

echo "Purchasing 'moby-dick' (quantity: 1)..."
"$AEPCLI" "$API" book --publisher acme-books :purchase moby-dick \
  --quantity 1
echo ""

# --- Step 5: Verify results ---
echo "--- Verifying results ---"

echo "Getting 'great-gatsby' (expect published=true, purchase_count=5):"
"$AEPCLI" "$API" book --publisher acme-books get great-gatsby
echo ""

echo "Getting 'moby-dick' (expect published=false, purchase_count=1):"
"$AEPCLI" "$API" book --publisher acme-books get moby-dick
echo ""

echo "=== Demo complete ==="
