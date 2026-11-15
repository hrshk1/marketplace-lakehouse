# 4. SCD Type 2 only on the customer dimension

Status: accepted

## Context

Dimensions change over time. Type 1 overwrites the old value; Type 2 keeps one row per version
with a validity range. Type 2 costs more: surrogate keys per version, a point-in-time join in
every fact load, and analysts must know which version they are looking at.

The business questions that depend on history are about customers: revenue by the city a customer
lived in when they ordered, and behaviour by the loyalty tier they held at the time.

## Decision

- `dim_customer` is Type 2 on `city`, `state`, `pincode` and `loyalty_tier`.
- `dim_seller` and `dim_product` are Type 1.
- Facts store the price actually charged (`unit_price`), so a product price change never rewrites
  past revenue even though `dim_product` only shows the current list price.

## Consequences

- Fact loads resolve `customer_sk` with a range join on `valid_from` and `valid_to`.
- "Revenue by city" has one correct answer (join on `customer_sk`) and one tempting wrong answer
  (join through `customer_id` to the current row). Query 07 shows both.
- History before the first load is unknown: the first version of every customer is valid from
  1900-01-01, so orders older than the pipeline attach to the attributes seen at first load.
- Changes are captured at batch granularity. Two changes to the same customer between two runs
  produce one new version, not two.
- If seller ratings or product categories start to matter historically, the same merge can be
  reused; the cost is a point-in-time join for that key as well.
