#!/usr/bin/env bash
# Bookstore demo: uses aepcli for ALL operations.
#
# Prerequisites:
#   1. Install aepcli (e.g. go install github.com/aep-dev/aepcli/cmd/aepcli@latest)
#   2. Start server:  go run ./examples/bookstore

set -euo pipefail

API="http://localhost:8080/openapi.json"

if ! command -v aepcli &>/dev/null; then
  echo "aepcli not found in PATH"
  echo "Install it first: go install github.com/aep-dev/aepcli/cmd/aepcli@latest"
  exit 1
fi

echo "=== Bookstore Demo ==="
echo ""

# --- Step 1: Create resource definitions ---
echo "--- Creating resource definitions ---"

echo "Creating publisher definition..."
aepcli "$API" definition create publisher \
  --singular publisher \
  --plural publishers \
  --schema '{"type":"object","properties":{"name":{"type":"string"},"location":{"type":"string"}}}'

echo ""
echo "Creating book definition (child of publisher)..."
aepcli "$API" definition create book \
  --singular book \
  --plural books \
  --schema '{"type":"object","properties":{"title":{"type":"string"},"author":{"type":"string"},"published":{"type":"boolean"},"purchase_count":{"type":"integer"}}}' \
  --parents publisher

echo ""

# --- Step 2: Create publisher and book instances ---
echo "--- Creating resources ---"

echo "Creating publisher 'acme-books'..."
aepcli "$API" publisher create acme-books \
  --name "Acme Books" \
  --location "New York"

echo ""
echo "Creating book 'great-gatsby'..."
aepcli "$API" book --publisher acme-books create great-gatsby \
  --title "The Great Gatsby" \
  --author "F. Scott Fitzgerald" \
  --purchase_count 0

echo ""
echo "Creating book 'moby-dick'..."
aepcli "$API" book --publisher acme-books create moby-dick \
  --title "Moby Dick" \
  --author "Herman Melville" \
  --purchase_count 0

echo ""

# --- Step 3: Verify resources were created ---
echo "--- Listing resources ---"

echo "Publishers:"
aepcli "$API" publisher list
echo ""

echo "Books (under acme-books):"
aepcli "$API" book --publisher acme-books list
echo ""

# --- Step 4: Call custom methods ---
echo "--- Calling custom methods ---"

echo "Publishing 'great-gatsby'..."
aepcli "$API" book --publisher acme-books :publish great-gatsby
echo ""

echo "Purchasing 'great-gatsby' (quantity: 3)..."
aepcli "$API" book --publisher acme-books :purchase great-gatsby \
  --quantity 3
echo ""

echo "Purchasing 'great-gatsby' again (quantity: 2)..."
aepcli "$API" book --publisher acme-books :purchase great-gatsby \
  --quantity 2
echo ""

echo "Purchasing 'moby-dick' (quantity: 1)..."
aepcli "$API" book --publisher acme-books :purchase moby-dick \
  --quantity 1
echo ""

# --- Step 5: Verify results ---
echo "--- Verifying results ---"

echo "Getting 'great-gatsby' (expect published=true, purchase_count=5):"
aepcli "$API" book --publisher acme-books get great-gatsby
echo ""

echo "Getting 'moby-dick' (expect published=false, purchase_count=1):"
aepcli "$API" book --publisher acme-books get moby-dick
echo ""

echo "=== Demo complete ==="
