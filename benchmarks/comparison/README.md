# Aepbase vs OpenClaw Memory: Benchmark

Quantitative comparison of using Aepbase (structured API) vs OpenClaw-style
markdown memory for storing and querying structured data.

## What It Measures

| Metric | How |
|--------|-----|
| **Token cost** | Actual `input_tokens` / `output_tokens` from the Anthropic API |
| **Accuracy** | Whether the LLM answer contains the known-correct value |
| **Scaling** | Runs identical queries at 10, 50, and 200 records |

## The Two Approaches

**Aepbase**: The LLM receives only the filtered query results from the API.
For "find unreimbursed receipts over $100", it gets back only matching rows
as compact JSON. The prompt is small and constant regardless of total records.

**OpenClaw memory**: The LLM receives the entire markdown file containing all
receipts in context. It must parse and reason over all records to answer the
query. The prompt grows linearly with record count.

## Prerequisites

1. **Go 1.23+**
2. **Anthropic API key** — set `ANTHROPIC_API_KEY` in your environment

## Usage

```bash
# Dry run — estimates token counts without making API calls
./benchmarks/comparison/run.sh --dry-run

# Full run with default settings (10, 50, 200 records)
export ANTHROPIC_API_KEY=sk-ant-...
./benchmarks/comparison/run.sh

# Custom record counts
./benchmarks/comparison/run.sh --records 10,100,500

# JSON output for further analysis
./benchmarks/comparison/run.sh --json > results.json

# Use a different model
./benchmarks/comparison/run.sh --model claude-haiku-4-5-20251001
```

## Example Output

```
=== Aepbase vs OpenClaw Benchmark Results ===

Records  Query                       Approach  Input Tokens  Output Tokens  Correct  Latency (ms)
-------  -----                       --------  ------------  -------------  -------  ------------
10       count_unreimbursed_over_100  aepbase   320           8              YES      450
10       count_unreimbursed_over_100  openclaw  580           8              YES      420
50       count_unreimbursed_over_100  aepbase   510           8              YES      480
50       count_unreimbursed_over_100  openclaw  2100          8              YES      510
200      count_unreimbursed_over_100  aepbase   1200          8              YES      520
200      count_unreimbursed_over_100  openclaw  8400          15             NO       890

=== Summary by Record Count ===

Records  Approach  Total Input Tokens  Accuracy  Token Savings
-------  --------  ------------------  --------  -------------
200      aepbase   4500                5/5
200      openclaw  38000               3/5       88% fewer tokens
```

## Queries Tested

1. **count_unreimbursed_over_100** — Count receipts where `reimbursed==false && amount>100`
2. **provider_total** — Sum all amounts for a specific provider
3. **category_count** — Count receipts in a category
4. **largest_receipt** — Find the receipt ID with the highest amount
5. **count_by_year** — Count receipts in a specific year

## How It Works

1. Starts an **in-memory Aepbase server** (no external dependencies)
2. Creates an HSA receipt resource definition via the meta-API
3. Seeds deterministic receipt data (same seed = reproducible results)
4. For each query at each record count:
   - **Aepbase path**: Fetches filtered results via CEL query, includes only
     matching rows in the LLM prompt
   - **OpenClaw path**: Includes the entire markdown memory file in the LLM prompt
5. Sends both prompts to the Anthropic API, records `input_tokens`,
   `output_tokens`, correctness, and latency
6. Prints comparison table

## Key Insight

Aepbase's token cost for filtered queries stays roughly **constant** as data
grows — the CEL filter runs server-side, and only matching rows enter the
prompt. OpenClaw's token cost grows **linearly** because the entire memory
file must be loaded into context for the LLM to reason over.
