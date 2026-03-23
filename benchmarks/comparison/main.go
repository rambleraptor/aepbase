// Package main implements a benchmark comparing Aepbase (structured API) vs
// OpenClaw-style memory (markdown files) for storing and querying structured
// data like HSA receipts.
//
// It measures:
//   - Token cost: how many LLM tokens each approach consumes per operation
//   - Accuracy: whether the LLM returns correct answers for structured queries
//   - Scaling: how both approaches behave as record count grows
//
// Usage:
//
//	export ANTHROPIC_API_KEY=sk-ant-...
//	go run ./benchmarks/comparison [flags]
//
// The benchmark starts an in-memory Aepbase server, seeds data, then runs
// identical queries through both approaches via the Claude API, collecting
// token counts and checking correctness.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"net/http"
	"os"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/aep-dev/aepbase/pkg/aepbase"
)

// Receipt represents an HSA receipt used as seed data.
type Receipt struct {
	ID           string  `json:"id,omitempty"`
	Provider     string  `json:"provider"`
	Amount       float64 `json:"amount"`
	Date         string  `json:"date"`
	Category     string  `json:"category"`
	Reimbursed   bool    `json:"reimbursed"`
	Description  string  `json:"description"`
}

// Query defines a benchmark query with an expected answer for accuracy checking.
type Query struct {
	Name           string
	Prompt         string
	ExpectedAnswer string // substring that must appear in a correct answer
}

// Result captures metrics for a single query execution.
type Result struct {
	Query          string
	Approach       string // "aepbase" or "openclaw"
	RecordCount    int
	InputTokens    int
	OutputTokens   int
	TotalTokens    int
	Correct        bool
	RawAnswer      string
	LatencyMs      int64
	Error          string
}

var (
	providers  = []string{"Dr. Smith", "Dr. Jones", "CityHealth Clinic", "Walgreens", "CVS Pharmacy", "LabCorp", "Quest Diagnostics", "Lenscrafters"}
	categories = []string{"dental", "vision", "prescription", "lab", "office_visit", "therapy", "medical_equipment"}
	recordSizes = []int{10, 50, 200}

	flagModel      = flag.String("model", "claude-sonnet-4-20250514", "Anthropic model to use")
	flagRecords    = flag.String("records", "", "Comma-separated record counts (default: 10,50,200)")
	flagDryRun     = flag.Bool("dry-run", false, "Generate data and prompts but skip LLM calls; prints prompts instead")
	flagOutputJSON = flag.Bool("json", false, "Output results as JSON")
)

func main() {
	flag.Parse()

	if *flagRecords != "" {
		recordSizes = parseRecordSizes(*flagRecords)
	}

	apiKey := os.Getenv("ANTHROPIC_API_KEY")
	if apiKey == "" && !*flagDryRun {
		log.Fatal("ANTHROPIC_API_KEY environment variable is required (or use -dry-run)")
	}

	// Start an in-memory Aepbase server on a random port.
	port, stopServer := startAepbaseServer()
	defer stopServer()
	baseURL := fmt.Sprintf("http://localhost:%d", port)

	// Create the HSA receipt resource definition.
	createReceiptDefinition(baseURL)

	var allResults []Result

	for _, count := range recordSizes {
		fmt.Fprintf(os.Stderr, "\n=== Running benchmark with %d records ===\n", count)

		// Generate deterministic seed data.
		receipts := generateReceipts(count)

		// Seed Aepbase with the data.
		clearReceipts(baseURL)
		seedAepbase(baseURL, receipts)

		// Build the markdown memory file content.
		markdownMemory := buildMarkdownMemory(receipts)

		// Build queries with expected answers based on actual data.
		queries := buildQueries(receipts)

		for _, q := range queries {
			fmt.Fprintf(os.Stderr, "  Query: %s\n", q.Name)

			// --- Aepbase approach ---
			aepbasePrompt := buildAepbasePrompt(q, baseURL)
			aepResult := runQuery(apiKey, aepbasePrompt, q, "aepbase", count)
			allResults = append(allResults, aepResult)

			// --- OpenClaw approach ---
			openclawPrompt := buildOpenclawPrompt(q, markdownMemory)
			ocResult := runQuery(apiKey, openclawPrompt, q, "openclaw", count)
			allResults = append(allResults, ocResult)
		}
	}

	// Output results.
	if *flagOutputJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		enc.Encode(allResults)
	} else {
		printTable(allResults)
	}
}

// startAepbaseServer starts an in-memory Aepbase on a random port and returns
// the port and a stop function.
func startAepbaseServer() (int, func()) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	listener.Close()

	opts := aepbase.ServerOptions{
		Port:     port,
		InMemory: true,
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- aepbase.Run(opts)
	}()

	// Wait for server to be ready.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get(fmt.Sprintf("http://localhost:%d/openapi.json", port))
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == 200 {
				break
			}
		}
		time.Sleep(50 * time.Millisecond)
	}

	return port, func() {
		// Server runs until process exits; no graceful shutdown needed for benchmarks.
	}
}

// createReceiptDefinition creates the HSA receipt resource type in Aepbase.
func createReceiptDefinition(baseURL string) {
	def := map[string]any{
		"singular":    "receipt",
		"plural":      "receipts",
		"description": "An HSA receipt for a medical expense.",
		"schema": map[string]any{
			"type": "object",
			"properties": map[string]any{
				"provider":    map[string]any{"type": "string"},
				"amount":      map[string]any{"type": "number"},
				"date":        map[string]any{"type": "string"},
				"category":    map[string]any{"type": "string"},
				"reimbursed":  map[string]any{"type": "boolean"},
				"description": map[string]any{"type": "string"},
			},
			"required": []string{"provider", "amount", "date", "category"},
		},
	}
	body, _ := json.Marshal(def)
	resp, err := http.Post(baseURL+"/aep-resource-definitions?id=receipt", "application/json", bytes.NewReader(body))
	if err != nil {
		log.Fatalf("create receipt definition: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		b, _ := io.ReadAll(resp.Body)
		log.Fatalf("create receipt definition: status %d: %s", resp.StatusCode, b)
	}
}

// generateReceipts creates deterministic receipt data.
func generateReceipts(count int) []Receipt {
	rng := rand.New(rand.NewSource(42))
	receipts := make([]Receipt, count)
	for i := range receipts {
		year := 2024 + rng.Intn(2) // 2024 or 2025
		month := 1 + rng.Intn(12)
		day := 1 + rng.Intn(28)
		receipts[i] = Receipt{
			ID:          fmt.Sprintf("r%04d", i+1),
			Provider:    providers[rng.Intn(len(providers))],
			Amount:      float64(10+rng.Intn(990)) + float64(rng.Intn(100))/100.0,
			Date:        fmt.Sprintf("%04d-%02d-%02d", year, month, day),
			Category:    categories[rng.Intn(len(categories))],
			Reimbursed:  rng.Float64() < 0.4,
			Description: fmt.Sprintf("Medical expense #%d", i+1),
		}
	}
	return receipts
}

// clearReceipts deletes all existing receipts from Aepbase.
func clearReceipts(baseURL string) {
	// List all receipts.
	resp, err := http.Get(baseURL + "/receipts?max_page_size=1000")
	if err != nil {
		return // resource might not exist yet
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return
	}
	var listResp struct {
		Results []map[string]any `json:"results"`
	}
	json.NewDecoder(resp.Body).Decode(&listResp)

	for _, r := range listResp.Results {
		if id, ok := r["id"].(string); ok {
			req, _ := http.NewRequest("DELETE", baseURL+"/receipts/"+id, nil)
			http.DefaultClient.Do(req)
		}
	}
}

// seedAepbase inserts receipts into the running Aepbase server.
func seedAepbase(baseURL string, receipts []Receipt) {
	for _, r := range receipts {
		body, _ := json.Marshal(map[string]any{
			"provider":    r.Provider,
			"amount":      r.Amount,
			"date":        r.Date,
			"category":    r.Category,
			"reimbursed":  r.Reimbursed,
			"description": r.Description,
		})
		url := fmt.Sprintf("%s/receipts?id=%s", baseURL, r.ID)
		resp, err := http.Post(url, "application/json", bytes.NewReader(body))
		if err != nil {
			log.Fatalf("seed receipt %s: %v", r.ID, err)
		}
		resp.Body.Close()
		if resp.StatusCode != 200 {
			log.Fatalf("seed receipt %s: status %d", r.ID, resp.StatusCode)
		}
	}
}

// buildMarkdownMemory creates the OpenClaw-style markdown memory content.
func buildMarkdownMemory(receipts []Receipt) string {
	var sb strings.Builder
	sb.WriteString("# HSA Receipts\n\n")
	for _, r := range receipts {
		reimbursedStr := "No"
		if r.Reimbursed {
			reimbursedStr = "Yes"
		}
		fmt.Fprintf(&sb, "- **%s** | %s | $%.2f | %s | Reimbursed: %s | %s\n",
			r.ID, r.Provider, r.Amount, r.Date, reimbursedStr, r.Category)
	}
	return sb.String()
}

// buildQueries creates queries with expected answers computed from the data.
func buildQueries(receipts []Receipt) []Query {
	// Q1: Count unreimbursed receipts over $100
	unreimbursedOver100 := 0
	for _, r := range receipts {
		if !r.Reimbursed && r.Amount > 100 {
			unreimbursedOver100++
		}
	}

	// Q2: Total amount for a specific provider
	targetProvider := "Dr. Smith"
	var providerTotal float64
	for _, r := range receipts {
		if r.Provider == targetProvider {
			providerTotal += r.Amount
		}
	}

	// Q3: Count receipts by category
	targetCategory := "dental"
	dentalCount := 0
	for _, r := range receipts {
		if r.Category == targetCategory {
			dentalCount++
		}
	}

	// Q4: Find the largest receipt
	var largest Receipt
	for _, r := range receipts {
		if r.Amount > largest.Amount {
			largest = r
		}
	}

	// Q5: Count receipts in 2025
	count2025 := 0
	for _, r := range receipts {
		if strings.HasPrefix(r.Date, "2025") {
			count2025++
		}
	}

	return []Query{
		{
			Name:           "count_unreimbursed_over_100",
			Prompt:         "How many unreimbursed receipts have an amount greater than $100? Reply with ONLY the number, nothing else.",
			ExpectedAnswer: fmt.Sprintf("%d", unreimbursedOver100),
		},
		{
			Name:           "provider_total",
			Prompt:         fmt.Sprintf("What is the total dollar amount of all receipts from %s? Reply with ONLY the dollar amount rounded to two decimal places (e.g. $1234.56), nothing else.", targetProvider),
			ExpectedAnswer: fmt.Sprintf("%.2f", providerTotal),
		},
		{
			Name:           "category_count",
			Prompt:         fmt.Sprintf("How many receipts are in the %q category? Reply with ONLY the number, nothing else.", targetCategory),
			ExpectedAnswer: fmt.Sprintf("%d", dentalCount),
		},
		{
			Name:           "largest_receipt",
			Prompt:         "What is the ID of the receipt with the largest amount? Reply with ONLY the receipt ID (e.g. r0001), nothing else.",
			ExpectedAnswer: largest.ID,
		},
		{
			Name:           "count_by_year",
			Prompt:         "How many receipts are dated in the year 2025? Reply with ONLY the number, nothing else.",
			ExpectedAnswer: fmt.Sprintf("%d", count2025),
		},
	}
}

// buildAepbasePrompt constructs the system+user prompt for the Aepbase approach.
// The LLM gets tool descriptions for the Aepbase API but we simulate tool use
// by pre-fetching the relevant data. This measures the token cost of what the
// LLM would see in a real MCP flow.
func buildAepbasePrompt(q Query, baseURL string) string {
	// For each query, fetch the relevant filtered data from Aepbase and include
	// it as a "tool result" in the prompt, simulating an MCP tool call response.
	var toolResult string

	switch q.Name {
	case "count_unreimbursed_over_100":
		toolResult = fetchAepbase(baseURL, `/receipts?filter=reimbursed==false%20%26%26%20amount>100&max_page_size=1000`)
	case "provider_total":
		toolResult = fetchAepbase(baseURL, `/receipts?filter=provider=="Dr. Smith"&max_page_size=1000`)
	case "category_count":
		toolResult = fetchAepbase(baseURL, `/receipts?filter=category=="dental"&max_page_size=1000`)
	case "largest_receipt":
		// No server-side sort; fetch all and let LLM find max. Still only gets compact JSON.
		toolResult = fetchAepbase(baseURL, `/receipts?max_page_size=1000`)
	case "count_by_year":
		toolResult = fetchAepbase(baseURL, `/receipts?filter=date.startsWith("2025")&max_page_size=1000`)
	}

	return fmt.Sprintf(`You are a data assistant. You have access to an Aepbase API that stores HSA receipts.

The following is the result of querying the receipts API:

%s

Based on this data, answer the following question:

%s`, toolResult, q.Prompt)
}

// buildOpenclawPrompt constructs the prompt for the OpenClaw memory approach.
// The LLM gets the entire markdown memory file in context.
func buildOpenclawPrompt(q Query, markdownMemory string) string {
	return fmt.Sprintf(`You are a data assistant. You have access to the following memory file containing HSA receipts:

%s

Based on this data, answer the following question:

%s`, markdownMemory, q.Prompt)
}

// fetchAepbase makes a GET request to Aepbase and returns the response body.
func fetchAepbase(baseURL, path string) string {
	resp, err := http.Get(baseURL + path)
	if err != nil {
		return fmt.Sprintf(`{"error": "%v"}`, err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)

	// Compact the JSON to minimize tokens.
	var buf bytes.Buffer
	if json.Compact(&buf, b) == nil {
		return buf.String()
	}
	return string(b)
}

// runQuery sends a prompt to the Anthropic API and collects metrics.
func runQuery(apiKey, prompt string, q Query, approach string, recordCount int) Result {
	result := Result{
		Query:       q.Name,
		Approach:    approach,
		RecordCount: recordCount,
	}

	if *flagDryRun {
		promptTokens := estimateTokens(prompt)
		result.InputTokens = promptTokens
		result.OutputTokens = 10 // estimated short answer
		result.TotalTokens = promptTokens + 10
		result.RawAnswer = "(dry-run)"
		result.Correct = false
		fmt.Fprintf(os.Stderr, "    [%s] ~%d input tokens (dry-run)\n", approach, promptTokens)
		return result
	}

	start := time.Now()

	reqBody, _ := json.Marshal(map[string]any{
		"model":      *flagModel,
		"max_tokens": 100,
		"messages": []map[string]any{
			{"role": "user", "content": prompt},
		},
	})

	req, _ := http.NewRequestWithContext(context.Background(), "POST", "https://api.anthropic.com/v1/messages", bytes.NewReader(reqBody))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-api-key", apiKey)
	req.Header.Set("anthropic-version", "2023-06-01")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		result.Error = err.Error()
		return result
	}
	defer resp.Body.Close()

	var apiResp struct {
		Content []struct {
			Text string `json:"text"`
		} `json:"content"`
		Usage struct {
			InputTokens  int `json:"input_tokens"`
			OutputTokens int `json:"output_tokens"`
		} `json:"usage"`
		Error *struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&apiResp); err != nil {
		result.Error = fmt.Sprintf("decode response: %v", err)
		return result
	}
	if apiResp.Error != nil {
		result.Error = apiResp.Error.Message
		return result
	}

	result.LatencyMs = time.Since(start).Milliseconds()
	result.InputTokens = apiResp.Usage.InputTokens
	result.OutputTokens = apiResp.Usage.OutputTokens
	result.TotalTokens = result.InputTokens + result.OutputTokens

	if len(apiResp.Content) > 0 {
		result.RawAnswer = strings.TrimSpace(apiResp.Content[0].Text)
	}

	// Check correctness: expected answer must appear in the response.
	result.Correct = strings.Contains(result.RawAnswer, q.ExpectedAnswer)

	fmt.Fprintf(os.Stderr, "    [%s] %d input, %d output tokens | correct=%v | answer=%q\n",
		approach, result.InputTokens, result.OutputTokens, result.Correct, result.RawAnswer)

	return result
}

// estimateTokens gives a rough token estimate (~4 chars per token).
func estimateTokens(s string) int {
	return len(s) / 4
}

// parseRecordSizes parses a comma-separated list of integers.
func parseRecordSizes(s string) []int {
	parts := strings.Split(s, ",")
	var sizes []int
	for _, p := range parts {
		p = strings.TrimSpace(p)
		var n int
		fmt.Sscanf(p, "%d", &n)
		if n > 0 {
			sizes = append(sizes, n)
		}
	}
	sort.Ints(sizes)
	return sizes
}

// printTable outputs results as a formatted table.
func printTable(results []Result) {
	fmt.Println()
	fmt.Println("=== Aepbase vs OpenClaw Benchmark Results ===")
	fmt.Println()

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "Records\tQuery\tApproach\tInput Tokens\tOutput Tokens\tCorrect\tLatency (ms)\tError")
	fmt.Fprintln(w, "-------\t-----\t--------\t------------\t-------------\t-------\t------------\t-----")

	for _, r := range results {
		correctStr := "YES"
		if !r.Correct {
			correctStr = "NO"
		}
		if r.Error != "" {
			correctStr = "ERR"
		}
		errStr := r.Error
		if len(errStr) > 40 {
			errStr = errStr[:40] + "..."
		}
		fmt.Fprintf(w, "%d\t%s\t%s\t%d\t%d\t%s\t%d\t%s\n",
			r.RecordCount, r.Query, r.Approach,
			r.InputTokens, r.OutputTokens, correctStr, r.LatencyMs, errStr)
	}
	w.Flush()

	// Print summary.
	fmt.Println()
	fmt.Println("=== Summary by Record Count ===")
	fmt.Println()

	type summary struct {
		count       int
		approach    string
		totalInput  int
		totalOutput int
		correct     int
		total       int
	}
	summaries := make(map[string]*summary)

	for _, r := range results {
		if r.Error != "" {
			continue
		}
		key := fmt.Sprintf("%d-%s", r.RecordCount, r.Approach)
		s, ok := summaries[key]
		if !ok {
			s = &summary{count: r.RecordCount, approach: r.Approach}
			summaries[key] = s
		}
		s.totalInput += r.InputTokens
		s.totalOutput += r.OutputTokens
		s.total++
		if r.Correct {
			s.correct++
		}
	}

	w2 := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w2, "Records\tApproach\tTotal Input Tokens\tAccuracy\tToken Savings")
	fmt.Fprintln(w2, "-------\t--------\t------------------\t--------\t-------------")

	for _, count := range recordSizes {
		aKey := fmt.Sprintf("%d-aepbase", count)
		oKey := fmt.Sprintf("%d-openclaw", count)
		a, aOk := summaries[aKey]
		o, oOk := summaries[oKey]
		if !aOk || !oOk {
			continue
		}
		savings := ""
		if o.totalInput > 0 {
			pct := 100.0 * float64(o.totalInput-a.totalInput) / float64(o.totalInput)
			savings = fmt.Sprintf("%.0f%% fewer tokens", pct)
		}
		fmt.Fprintf(w2, "%d\t%s\t%d\t%d/%d\t\n", count, "aepbase", a.totalInput, a.correct, a.total)
		fmt.Fprintf(w2, "%d\t%s\t%d\t%d/%d\t%s\n", count, "openclaw", o.totalInput, o.correct, o.total, savings)
	}
	w2.Flush()
}
