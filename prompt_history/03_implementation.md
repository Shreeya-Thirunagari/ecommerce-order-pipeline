# Implementation History – YottaFlex Pipeline

## Technology Stack

- Python 3.10+
- Pandas
- CSV
- PyYAML
- Pytest
- Git and GitHub

## Pipeline Architecture

The project follows a layered data processing approach:

`Incoming → Raw → Validated → Curated`

Invalid records are routed separately:

`Validated Processing → Quarantine`

## Main Implementation Components

### 1. Ingestion

- Reads incoming CSV files.
- Calculates file checksums.
- Tracks source file metadata.
- Prevents duplicate file processing.

### 2. Validation

- Checks required fields.
- Validates data types and timestamps.
- Checks quantity, price, discount, and status values.
- Identifies invalid records.
- Captures rejection reasons.

### 3. Transformation

- Removes duplicate orders.
- Keeps the latest update using `updated_at`.
- Handles late-arriving records.
- Produces curated CSV files partitioned by order date.

### 4. Pipeline Orchestration

- Coordinates the processing stages.
- Tracks execution status.
- Handles file-level failures.
- Supports repeatable pipeline execution.

### 5. Metrics and Logging

Tracks information such as:

- Number of input records.
- Number of accepted records.
- Number of rejected records.
- Duplicate file deliveries.
- Pipeline execution status.
- Processing duration.

## Testing Strategy

The project includes tests for:

- Successful pipeline execution.
- Invalid records.
- Duplicate orders.
- Late-arriving data.
- Duplicate file delivery.
- Reprocessing and idempotency.
- Malformed CSV files.
- Empty input scenarios.

## Sample and Large Dataset Generation

The project includes scripts to generate sample e-commerce order records and larger datasets for testing.

## Design Principles

- Keep the implementation modular.
- Avoid unnecessary complexity.
- Make processing deterministic.
- Preserve source data.
- Separate validation from transformation.
- Make failures observable.
- Ensure repeated runs do not create duplicate curated records.
