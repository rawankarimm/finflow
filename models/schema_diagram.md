# Schema Diagram — FinFlow

ER diagram for the transactions schema (Option 2: Snowflake on the Account dimension).
Rendered with [Mermaid]( https://mermaid.js.org/syntax/entityRelationshipDiagram.html).

```mermaid
erDiagram
    dim_transaction_type ||--o{ fact_transactions : "categorizes"
    dim_time ||--o{ fact_transactions : "occurs during"
    dim_account ||--o{ fact_transactions : "sends (sender_account_id)"
    dim_account ||--o{ fact_transactions : "receives (receiver_account_id)"
    dim_account_type ||--o{ dim_account : "classifies"

    dim_transaction_type {
        int transaction_type_id PK
        varchar transaction_type_name
    }

    dim_account_type {
        int account_type_id PK
        varchar type_name
    }

    dim_account {
        int account_id PK
        varchar account_name
        int account_type_id FK
    }

    dim_time {
        int step PK
        int sim_day
        int sim_week
        int hour_of_day
    }

    fact_transactions {
        int transaction_id PK
        int transaction_type_id FK
        int step FK
        int sender_account_id FK
        int receiver_account_id FK
        float amount
        float log_amount
        float balance_drain
        boolean is_fraud
        boolean is_flagged_fraud
        float old_balance_sender
        float new_balance_sender
        float old_balance_receiver
        float new_balance_receiver
    }

    complaints {
        int complaint_id PK
        date date_received
        varchar product
        varchar sub_product
        varchar issue
        varchar company
        char state
        varchar resolution
    }
```

**Notes on the diagram:**
- `complaints` is intentionally disconnected — no foreign key ties it to `fact_transactions`. It comes from a different source (CFPB) and has no shared key with the transactions data.

- `dim_account` plays two roles on `fact_transactions` (sender and receiver).

- The `dim_account_type → dim_account` link is the snowflake: account type is normalized out of `dim_account` into its own table instead of being stored as a plain column there.
