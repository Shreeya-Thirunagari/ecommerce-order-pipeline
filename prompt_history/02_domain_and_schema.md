# Domain and Data Schema – YottaFlex

## Business Domain

The project uses an **E-commerce Order Management** scenario.

The pipeline processes customer orders, product information, order amounts, payment statuses, and order statuses.

## Storage Format

CSV is used as the storage format across the pipeline layers.

## Order Data Schema

| Column | Description |
|---|---|
| order_id | Unique order identifier |
| customer_id | Customer identifier |
| product_id | Product identifier |
| order_date | Date the order was placed |
| updated_at | Last update timestamp |
| quantity | Number of products ordered |
| unit_price | Price per product |
| discount | Discount applied |
| payment_status | Payment status |
| order_status | Order processing status |
| region | Customer or order region |
| total_amount | Final order amount |

## Data Validation Rules

- Order ID must not be null.
- Customer ID must not be null.
- Product ID must not be null.
- Order date and updated timestamp must be valid.
- Quantity must be greater than zero.
- Unit price must be positive.
- Discount must not be negative or exceed the gross amount.
- Payment status must belong to the allowed values.
- Order status must belong to the allowed values.
- Total amount must match the expected calculation.

## Allowed Payment Statuses

- SUCCESS
- PENDING
- FAILED
- REFUNDED

## Allowed Order Statuses

- PLACED
- PROCESSING
- SHIPPED
- DELIVERED
- CANCELLED

## Deduplication Strategy

The same order may arrive multiple times with different updates.

The pipeline uses:

- Business key: `order_id`
- Winner column: `updated_at`
- Rule: Keep the record with the latest `updated_at`.
- Tie-breaking: Use deterministic source-file and record-hash ordering.

## Data Layers

### Raw
Preserves incoming source data for traceability.

### Validated
Contains records that pass validation.

### Quarantine
Contains rejected records and their rejection reasons.

### Curated
Contains cleaned, deduplicated, analytics-ready order data.

## Late-Arriving Data

Late-arriving records are supported. Curated partitions are rebuilt from validated data so that older order dates can be incorporated even when the records arrive later.
