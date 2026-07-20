# Why Snowflake Schema

Star and snowflake schemas are both dimensional models — both organize
data around a fact table surrounded by dimension tables. The difference
comes down to how the dimensions themselves are structured: a star schema
keeps each dimension flat and denormalized, while a snowflake schema
splits a dimension further into related sub-tables when it makes sense to.

For this project, I went with a snowflake approach for the account
dimension specifically, rather than keeping everything fully flat.

## Why

We're working with a large, frequently-changing dataset, so keeping
redundancy low and enforcing referential integrity on master data matters
more here than it would on a small, static dataset.

Account type (individual vs. merchant) is a reusable, low-cardinality
attribute — a small set of values shared across many accounts — which
makes it a good candidate to normalize out into its own table instead of
repeating the label on every account row. This also matters for the
project's fraud-analysis use case, since individual-to-individual and
individual-to-merchant transactions tend to show different fraud patterns,
so having account type modeled cleanly makes that kind of analysis easier
downstream.

## What this gets us

- **Updates touch one place, not many.** If an account type gets renamed
  or reclassified, it's a single row update in the account type table
  instead of updating every account that shares that type.
- **Less duplication.** Each account type value is stored once instead of
  being repeated across every account row, which keeps storage and the
  risk of inconsistent data lower.

The trade-off is one extra join when querying account type alongside
transactions — acceptable here given the benefits above.
