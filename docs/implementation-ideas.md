# Implementation Ideas for aepbase

Inspired by the [AEP spec](https://aep.dev/aep_list/), here are five features
that would bring aepbase closer to full AEP compliance.

---

## 1. Soft Delete (AEP-164)

Instead of permanently removing resources on DELETE, mark them as deleted and
allow recovery.

**What to build:**
- Add a `delete_time` output-only field to resources whose definition opts in
  (e.g. `"softDelete": true` on the ResourceDefinition).
- On DELETE, set `delete_time` instead of removing the row.
- GET a soft-deleted resource returns **410 Gone** by default.
- Add `show_deleted` query parameter to List and Get so clients can view
  soft-deleted resources.
- Add an `:undelete` custom method to restore soft-deleted resources.
- Optional: automatic purge after a configurable TTL.

**Why it's valuable:** Many real-world APIs need an undo/recycle-bin pattern.
This is one of the most requested features in resource-oriented APIs.

**Spec:** <https://aep.dev/164/>

---

## 2. Etags & Optimistic Concurrency (AEP-154)

Prevent lost-update problems by adding etag-based preconditions.

**What to build:**
- Compute an etag for every resource (e.g. hash of `update_time` + row
  contents, or a simple version counter).
- Return the `etag` field in all Get/List/Create/Update responses.
- Accept an `If-Match` header (or `etag` query param) on Update, Delete, and
  Apply requests.
- Return **412 Precondition Failed** (or 409 Aborted per AEP) when the etag
  does not match.
- Optionally support `If-None-Match: *` on Create to guarantee uniqueness
  without user-settable IDs.

**Why it's valuable:** Concurrent editing is common in collaborative or
multi-service environments. Etags are a lightweight way to avoid silent
data loss.

**Spec:** <https://aep.dev/154/>

---

## 3. Partial Responses / Field Masks (AEP-157)

Let clients request only the fields they need.

**What to build:**
- Accept a `read_mask` (or `fields`) query parameter on Get and List.
- Parse the comma-separated field paths and project the response to only
  include those fields (plus standard fields like `path`).
- On Update, use an explicit `update_mask` parameter so the server knows which
  fields the client intends to modify vs. which were simply omitted.
- Validate that mask paths refer to real fields in the resource schema.

**Why it's valuable:** Reduces payload size for large resources, and
`update_mask` removes the ambiguity of "is this field absent because the client
wants to clear it, or because it was omitted?"

**Spec:** <https://aep.dev/157/>

---

## 4. Long-Running Operations (AEP-151)

Support asynchronous request processing with pollable operation resources.

**What to build:**
- Define an `_operations` table with columns: `id`, `path`, `done` (bool),
  `result` (JSON), `error` (JSON), `metadata` (JSON), `create_time`,
  `update_time`.
- Allow custom methods (AEP-136) to return an Operation instead of an
  immediate result by declaring `"longRunning": true`.
- Expose `GET /operations/{id}` to poll status and
  `POST /operations/{id}:wait` to long-poll.
- When the operation completes (via a background goroutine or callback), store
  the result and set `done = true`.
- Optionally support `DELETE /operations/{id}` to clean up.

**Why it's valuable:** Some actions (report generation, data import, external
calls) take too long for a synchronous response. LROs are the AEP-standard way
to handle this.

**Spec:** <https://aep.dev/151/>

---

## 5. Batch Methods (AEP-231/233/234/235)

Allow clients to operate on multiple resources in a single request.

**What to build:**
- Add batch endpoints for each standard method:
  - `GET /publishers/books:batchGet` — retrieve multiple resources by ID.
  - `POST /publishers/books:batchCreate` — create several resources at once.
  - `PATCH /publishers/books:batchUpdate` — update several resources at once.
  - `POST /publishers/books:batchDelete` — delete several resources at once.
- Each request body contains an array of individual requests; each response
  contains a parallel array of results.
- Wrap the DB operations in a transaction for atomicity (all-or-nothing).
- Respect the same validation, etag, and soft-delete logic as the single-
  resource methods.

**Why it's valuable:** Batch endpoints dramatically reduce round-trips for
clients that need to work with many resources at once (bulk import, sync
scenarios, admin tooling).

**Spec:** <https://aep.dev/231/>, <https://aep.dev/233/>, <https://aep.dev/234/>, <https://aep.dev/235/>
