# Design & Trade-off Notes

## Why this architecture?

The classic **raw → validated → curated** medallion pattern was chosen because it is industry-standard, easy to explain, and gives clear separation of concerns:

- **Raw** is the immutable landing zone (audit + replay source of truth).
- **Validated** proves which records passed schema/business rules and keeps an easy-to-inspect CSV audit trail.
- **Curated** is the analytics-ready layer (deduplicated, partitioned, typed).

## Key design choices

1. **Content-hash idempotency**  
   Filename-based tracking is fragile (renames, re-uploads). Hashing the file body lets us detect true duplicate delivery. Cost is one pass over the file; acceptable for the target scale.

2. **Pydantic for validation**  
   Gives excellent, field-level error messages that become the `rejection_reason` in quarantine. Slightly slower than pure vectorised Pandas checks, but the clarity and maintainability win for a 0–3 yo audience.

3. **Keep-latest on (order_id, product_id)**  
   Matches the most common analytics need (“what is the current state of this order line?”). History of intermediate states is intentionally discarded; a slowly-changing-dimension design would be a natural next step.

4. **Hive-style CSV partitions**  
   Zero extra dependencies, human-readable, works with any tool that understands folders. Parquet would be more efficient for large data; the code is structured so swapping the writer is straightforward.

5. **Single-process, local folders**  
   Removes all operational complexity so the focus stays on data-quality, idempotency and observability. A production version would replace the folders with S3/MinIO + a proper table format.

## What was deliberately left out

- Distributed execution / locking
- Schema evolution / versioning
- Automated repair of quarantined records
- Streaming / micro-batch ingestion
- Full Great-Expectations-style DQ suite

These are listed as “Possible next steps” in the README so the learner can extend the project.
