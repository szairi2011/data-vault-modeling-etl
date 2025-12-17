# Data Vault 2.0 Banking POC - Entity Relationship Models

## Overview
This document presents the Entity Relationship Diagrams (ERD) for all four data layers in the Data Vault 2.0 banking POC. Each layer serves a specific purpose in the modern data warehouse architecture.

---

## Model 1: Source System (3NF Normalized - OLTP)

### Purpose
Represents the operational banking system following Third Normal Form (3NF) principles. This is the **source of truth** for all banking operations.

### Key Characteristics
- **Normalized structure** to eliminate data redundancy
- **Referential integrity** enforced through foreign keys
- **Transaction headers with line items** (e-commerce pattern)
- **CDC tracking** via `updated_at` timestamps
- **Business keys** for Data Vault integration

### ERD: Source System

```
┌─────────────────────────────┐
│       CUSTOMER              │
│─────────────────────────────│
│ PK customer_id (INT)        │
│ UK customer_number (VARCHAR)│ ◄────Business Key────┐
│    customer_type (VARCHAR)  │                      │
│    first_name (VARCHAR)     │                      │
│    last_name (VARCHAR)      │                      │
│    business_name (VARCHAR)  │                      │
│    email (VARCHAR)          │                      │
│    phone (VARCHAR)          │                      │
│    date_of_birth (DATE)     │                      │
│    ssn (VARCHAR)            │                      │
│    tax_id (VARCHAR)         │                      │
│    credit_score (INT)       │                      │
│    customer_since (DATE)    │                      │
│    loyalty_tier (VARCHAR)   │ ◄────Schema Evolution Example
│    preferred_contact (VARCHAR)│                    │
│    created_at (TIMESTAMP)   │                      │
│    updated_at (TIMESTAMP)   │ ◄────CDC Tracking────│
└─────────────────────────────┘                      │
              │ 1                                    │
              │                                      │
              │ *                                    │
┌─────────────▼───────────────┐                      │
│       ACCOUNT               │                      │
│─────────────────────────────│                      │
│ PK account_id (INT)         │                      │
│ UK account_number (VARCHAR) │ ◄────Business Key    │
│ FK customer_id (INT)        │                      │
│ FK product_id (INT)         │                      │
│ FK branch_id (INT)          │                      │
│    account_status (VARCHAR) │                      │
│    current_balance (DECIMAL)│                      │
│    available_balance (DEC)  │                      │
│    currency (VARCHAR)       │                      │
│    overdraft_limit (DECIMAL)│                      │
│    interest_rate (DECIMAL)  │                      │
│    opened_date (DATE)       │                      │
│    closed_date (DATE)       │                      │
│    last_transaction (TS)    │                      │
│    created_at (TIMESTAMP)   │                      │
│    updated_at (TIMESTAMP)   │ ◄────CDC Tracking    │
└─────────────────────────────┘                      │
              │ 1                                    │
              │                                      │
              │ *                                    │
┌─────────────▼───────────────┐                      │
│   TRANSACTION_HEADER        │                      │
│─────────────────────────────│                      │
│ PK transaction_id (INT)     │                      │
│ UK transaction_number (VAR) │ ◄────Business Key    │
│ FK account_id (INT)         │                      │
│    transaction_type (VAR)   │                      │
│    transaction_status (VAR) │                      │
│    total_amount (DECIMAL)   │ ◄────Sum of Items────┐
│    transaction_date (TS)    │                      │
│    posting_date (TIMESTAMP) │                      │
│    channel (VARCHAR)        │                      │
│    location (VARCHAR)       │                      │
│    description (TEXT)       │                      │
│    reference_number (VAR)   │                      │
│    initiated_by (VARCHAR)   │                      │
│    created_at (TIMESTAMP)   │                      │
│    updated_at (TIMESTAMP)   │                      │
└─────────────────────────────┘                      │
              │ 1                                    │
              │                                      │
              │ * ◄────E-COMMERCE PATTERN            │
┌─────────────▼───────────────┐                      │
│   TRANSACTION_ITEM          │                      │
│─────────────────────────────│                      │
│ PK item_id (INT)            │                      │
│ FK transaction_id (INT)     │                      │
│    item_sequence (INT)      │ ◄────Line Number     │
│ FK category_id (INT)        │                      │
│    item_amount (DECIMAL)    │ ────────────────────┘
│    item_description (TEXT)  │
│    payee_name (VARCHAR)     │
│    payee_account (VARCHAR)  │
│    merchant_name (VARCHAR)  │
│    merchant_category (VAR)  │
│    is_recurring (BOOLEAN)   │
│    created_at (TIMESTAMP)   │
└─────────────────────────────┘


┌─────────────────────────────┐     ┌─────────────────────────────┐
│       BRANCH                │     │       PRODUCT               │
│─────────────────────────────│     │─────────────────────────────│
│ PK branch_id (INT)          │     │ PK product_id (INT)         │
│ UK branch_code (VARCHAR)    │     │ UK product_code (VARCHAR)   │
│    branch_name (VARCHAR)    │     │    product_name (VARCHAR)   │
│    branch_type (VARCHAR)    │     │    product_category (VAR)   │
│    address_line1 (VARCHAR)  │     │    product_type (VARCHAR)   │
│    address_line2 (VARCHAR)  │     │    interest_rate (DECIMAL)  │
│    city (VARCHAR)           │     │    minimum_balance (DECIMAL)│
│    state (VARCHAR)          │     │    monthly_fee (DECIMAL)    │
│    zip_code (VARCHAR)       │     │    overdraft_limit (DEC)    │
│    country (VARCHAR)        │     │    description (TEXT)       │
│    phone (VARCHAR)          │     │    is_active (BOOLEAN)      │
│    manager_name (VARCHAR)   │     │    created_at (TIMESTAMP)   │
│    opening_date (DATE)      │     │    updated_at (TIMESTAMP)   │
│    is_active (BOOLEAN)      │     └─────────────────────────────┘
│    created_at (TIMESTAMP)   │              │
└─────────────────────────────┘              │
              │                              │
              └───────────┬──────────────────┘
                          │
                          │ Referenced by ACCOUNT
                          ▼

┌─────────────────────────────┐
│  TRANSACTION_CATEGORY       │
│─────────────────────────────│
│ PK category_id (INT)        │
│ UK category_code (VARCHAR)  │
│    category_name (VARCHAR)  │
│ FK parent_category_id (INT) │ ◄────Hierarchical
│    description (TEXT)       │
│    is_active (BOOLEAN)      │
└─────────────────────────────┘
              │
              └──────┐ Self-referencing
                     │ for hierarchy
              ┌──────▼───────────────────┐
              │  Parent-Child Categories │
              │  - Shopping              │
              │    ├── Groceries         │
              │    └── Clothing          │
              │  - Utilities             │
              │    ├── Electricity       │
              │    └── Water             │
              └──────────────────────────┘
```

### Relationships
- **Customer** → **Account**: One-to-Many (1:*)
  - One customer can have multiple accounts
- **Account** → **Transaction Header**: One-to-Many (1:*)
  - One account can have multiple transactions
- **Transaction Header** → **Transaction Item**: One-to-Many (1:*) 
  - **KEY FEATURE**: One transaction can have multiple items (e-commerce pattern)
- **Branch** → **Account**: One-to-Many (1:*)
  - One branch manages multiple accounts
- **Product** → **Account**: One-to-Many (1:*)
  - One product type can be used for multiple accounts
- **Transaction Category**: Hierarchical self-reference
  - Categories can have parent categories

### Business Keys (for Data Vault)
- `customer_number`: CUS-000001
- `account_number`: ACC-000000001
- `transaction_number`: TXN-2025-000001
- `branch_code`: BR-001
- `product_code`: PROD-CHK-001
- `category_code`: CAT-GROCERY

---

## Model 2: Raw Vault (Bronze Layer - Data Vault 2.0)

### Purpose
The **Raw Vault** is the foundation of Data Vault 2.0 architecture. It stores:
- **Immutable historical data** (insert-only)
- **Business keys** in Hubs
- **Relationships** in Links
- **Descriptive attributes** in Satellites

### Key Characteristics
- **No data loss** - all source changes are preserved
- **Auditability** - load timestamps and record sources
- **Schema evolution resilience** - new attributes added to satellites
- **Business key driven** - natural keys, not surrogate keys

### ERD: Raw Vault

```
┌─────────────────────────────┐
│      HUB_CUSTOMER           │    ◄────Business Entity
│─────────────────────────────│
│ PK hub_customer_hk (VARCHAR)│    ◄────MD5(customer_number)
│    customer_number (VARCHAR)│    ◄────Business Key
│    load_date (TIMESTAMP)    │    ◄────Audit: When loaded
│    record_source (VARCHAR)  │    ◄────Audit: Where from
└─────────────────────────────┘
              │ 1
              │
              │ *
┌─────────────▼───────────────┐
│    SAT_CUSTOMER             │    ◄────Descriptive Attributes
│─────────────────────────────│
│ PK hub_customer_hk (VARCHAR)│    ◄────Foreign Key to Hub
│ PK load_date (TIMESTAMP)    │    ◄────Composite PK (Type 2 SCD)
│    first_name (VARCHAR)     │
│    last_name (VARCHAR)      │
│    email (VARCHAR)          │
│    phone (VARCHAR)          │
│    date_of_birth (DATE)     │
│    ssn (VARCHAR)            │
│    loyalty_tier (VARCHAR)   │    ◄────NEW: Schema Evolution
│    preferred_contact (VAR)  │    ◄────NEW: Schema Evolution
│    hash_diff (VARCHAR)      │    ◄────MD5(all attributes)
│    record_source (VARCHAR)  │
└─────────────────────────────┘


┌─────────────────────────────┐
│      HUB_ACCOUNT            │
│─────────────────────────────│
│ PK hub_account_hk (VARCHAR) │    ◄────MD5(account_number)
│    account_number (VARCHAR) │    ◄────Business Key
│    load_date (TIMESTAMP)    │
│    record_source (VARCHAR)  │
└─────────────────────────────┘
              │ 1
              │
              │ *
┌─────────────▼───────────────┐
│    SAT_ACCOUNT              │
│─────────────────────────────│
│ PK hub_account_hk (VARCHAR) │
│ PK load_date (TIMESTAMP)    │
│    current_balance (DECIMAL)│
│    available_balance (DEC)  │
│    account_status (VARCHAR) │
│    currency (VARCHAR)       │
│    overdraft_limit (DECIMAL)│
│    interest_rate (DECIMAL)  │
│    opened_date (DATE)       │
│    closed_date (DATE)       │
│    hash_diff (VARCHAR)      │
│    record_source (VARCHAR)  │
└─────────────────────────────┘


┌─────────────────────────────┐
│   HUB_TRANSACTION           │
│─────────────────────────────│
│ PK hub_transaction_hk (VAR) │    ◄────MD5(transaction_number)
│    transaction_number (VAR) │    ◄────Business Key
│    load_date (TIMESTAMP)    │
│    record_source (VARCHAR)  │
└─────────────────────────────┘
              │ 1
              ├────────────────┐
              │ *              │ *
┌─────────────▼───────────┐   │
│   SAT_TRANSACTION       │   │
│─────────────────────────│   │
│ PK hub_transaction_hk   │   │
│ PK load_date (TIMESTAMP)│   │
│    transaction_type (VAR│   │
│    transaction_status   │   │
│    total_amount (DEC)   │   │
│    transaction_date (TS)│   │
│    posting_date (TS)    │   │
│    channel (VARCHAR)    │   │
│    location (VARCHAR)   │   │
│    description (TEXT)   │   │
│    hash_diff (VARCHAR)  │   │
│    record_source (VAR)  │   │
└─────────────────────────┘   │
                              │
                              │
┌─────────────────────────────▼─────────────┐
│   HUB_TRANSACTION_ITEM                    │
│───────────────────────────────────────────│
│ PK hub_transaction_item_hk (VARCHAR)      │    ◄────MD5(txn_number||item_seq)
│    transaction_number (VARCHAR)           │    ◄────Business Key Part 1
│    item_sequence (INT)                    │    ◄────Business Key Part 2
│    load_date (TIMESTAMP)                  │
│    record_source (VARCHAR)                │
└───────────────────────────────────────────┘
              │ 1
              │
              │ *
┌─────────────▼───────────────┐
│   SAT_TRANSACTION_ITEM      │
│─────────────────────────────│
│ PK hub_transaction_item_hk  │
│ PK load_date (TIMESTAMP)    │
│    item_amount (DECIMAL)    │
│    item_description (TEXT)  │
│    payee_name (VARCHAR)     │
│    merchant_name (VARCHAR)  │
│    merchant_category (VAR)  │
│    is_recurring (BOOLEAN)   │
│    hash_diff (VARCHAR)      │
│    record_source (VARCHAR)  │
└─────────────────────────────┘


┌─────────────────────────────────────────────────┐
│   LINK_CUSTOMER_ACCOUNT                         │   ◄────Relationship
│─────────────────────────────────────────────────│
│ PK link_customer_account_hk (VARCHAR)           │   ◄────MD5(hub_cust||hub_acct)
│ FK hub_customer_hk (VARCHAR)                    │
│ FK hub_account_hk (VARCHAR)                     │
│    load_date (TIMESTAMP)                        │
│    record_source (VARCHAR)                      │
└─────────────────────────────────────────────────┘


┌─────────────────────────────────────────────────┐
│   LINK_TRANSACTION_ITEM                         │   ◄────Parent-Child Link
│─────────────────────────────────────────────────│
│ PK link_transaction_item_hk (VARCHAR)           │   ◄────MD5(hub_txn||hub_item)
│ FK hub_transaction_hk (VARCHAR)                 │
│ FK hub_transaction_item_hk (VARCHAR)            │
│    load_date (TIMESTAMP)                        │
│    record_source (VARCHAR)                      │
└─────────────────────────────────────────────────┘

      ◄────────────────────────────────────┐
                                           │
      Additional Hubs, Links, Satellites:  │
      - HUB_BRANCH + SAT_BRANCH           │
      - HUB_PRODUCT + SAT_PRODUCT         │
      - HUB_CATEGORY + SAT_CATEGORY       │
      - LINK_ACCOUNT_BRANCH               │
      - LINK_ACCOUNT_PRODUCT              │
      - LINK_ITEM_CATEGORY                │
                                           │
      ◄────────────────────────────────────┘
```

### Data Vault 2.0 Components

#### Hubs (Business Entities)
- Store **business keys** only
- **Hash key (PK)**: MD5 hash of business key for performance
- **Load metadata**: timestamp and source system
- **Never change** - immutable structure

#### Satellites (Descriptive Data)
- Store **descriptive attributes**
- **Composite PK**: hub_hk + load_date (Type 2 SCD)
- **Hash diff**: MD5 of all attributes for change detection
- **Multiple versions** per hub (historical tracking)

#### Links (Relationships)
- Store **relationships between hubs**
- **Hash key (PK)**: MD5 of concatenated hub keys
- **Many-to-many** relationships supported
- **No descriptive attributes** (use link satellites if needed)

### Hash Key Generation
```scala
// Hub Customer Hash Key
MD5(customer_number) → "A3F5E8C9D1B2..."

// Link Customer-Account Hash Key
MD5(hub_customer_hk || hub_account_hk) → "D8E2F1C4A9B3..."

// Hash Diff for Satellite
MD5(first_name || last_name || email || ... || loyalty_tier) → "F2A8D3E1C5B9..."
```

### Schema Evolution Example
When `loyalty_tier` and `preferred_contact_method` are added to the source system:
1. **Hub remains unchanged** (only stores business key)
2. **New satellite records** are inserted with new columns
3. **Hash diff changes** for modified records
4. **No impact** on downstream systems until they're ready

---

## Model 3: Business Vault (Silver Layer - Analytics-Ready)

### Purpose
The **Business Vault** adds business logic and computed constructs on top of Raw Vault:
- **Point-in-Time (PIT) tables**: Temporal snapshots for efficient querying
- **Bridge tables**: Pre-joined many-to-many relationships
- **Reference tables**: Business classifications and hierarchies
- **Computed metrics**: Aggregations and business rules

### Key Characteristics
- **Read-optimized** for analytics
- **Denormalized** for performance
- **Business rules applied** (calculations, derivations)
- **Still auditable** - traces back to Raw Vault

### ERD: Business Vault

```
┌───────────────────────────────────────────────────┐
│            PIT_CUSTOMER                           │   ◄────Point-in-Time Snapshot
│───────────────────────────────────────────────────│
│ PK hub_customer_hk (VARCHAR)                      │
│ PK snapshot_date (DATE)                           │   ◄────Daily snapshot
│    first_name (VARCHAR)                           │
│    last_name (VARCHAR)                            │
│    email (VARCHAR)                                │
│    phone (VARCHAR)                                │
│    date_of_birth (DATE)                           │
│    ssn (VARCHAR)                                  │
│    loyalty_tier (VARCHAR)                         │
│    preferred_contact_method (VARCHAR)             │
│    sat_customer_load_date (TIMESTAMP)             │   ◄────When attributes were valid
│    load_date (TIMESTAMP)                          │   ◄────When PIT was created
└───────────────────────────────────────────────────┘

   ┌──────────────────────────────────────────────┐
   │  How PIT Works:                              │
   │                                              │
   │  Question: What did customer look like       │
   │            on 2025-01-15?                    │
   │                                              │
   │  Without PIT:                                │
   │  - Join hub_customer + sat_customer          │
   │  - Filter sat_customer.load_date <= '2025-01-15'│
   │  - Window function to get latest version     │
   │  - Expensive for many customers/dates        │
   │                                              │
   │  With PIT:                                   │
   │  - SELECT * FROM pit_customer                │
   │    WHERE snapshot_date = '2025-01-15'        │
   │  - Single table scan, much faster!           │
   └──────────────────────────────────────────────┘


┌───────────────────────────────────────────────────┐
│         PIT_ACCOUNT                               │
│───────────────────────────────────────────────────│
│ PK hub_account_hk (VARCHAR)                       │
│ PK snapshot_date (DATE)                           │
│    account_number (VARCHAR)                       │
│    current_balance (DECIMAL)                      │
│    available_balance (DECIMAL)                    │
│    account_status (VARCHAR)                       │
│    currency (VARCHAR)                             │
│    overdraft_limit (DECIMAL)                      │
│    interest_rate (DECIMAL)                        │
│    opened_date (DATE)                             │
│    closed_date (DATE)                             │
│    sat_account_load_date (TIMESTAMP)              │
│    load_date (TIMESTAMP)                          │
└───────────────────────────────────────────────────┘


┌───────────────────────────────────────────────────────────┐
│     BRIDGE_CUSTOMER_ACCOUNTS                              │   ◄────Pre-joined Bridge
│───────────────────────────────────────────────────────────│
│ PK bridge_key (VARCHAR)                                   │
│ FK hub_customer_hk (VARCHAR)                              │
│ FK hub_account_hk (VARCHAR)                               │
│    customer_number (VARCHAR)                              │
│    account_number (VARCHAR)                               │
│    account_status (VARCHAR)                               │
│    current_balance (DECIMAL)                              │
│    product_name (VARCHAR)                                 │
│    product_type (VARCHAR)                                 │
│    branch_name (VARCHAR)                                  │
│    is_active_account (BOOLEAN)                            │
│    opened_date (DATE)                                     │
│    closed_date (DATE)                                     │
│    load_date (TIMESTAMP)                                  │
└───────────────────────────────────────────────────────────┘

   ┌──────────────────────────────────────────────┐
   │  Why Bridges?                                │
   │                                              │
   │  Many-to-many relationships are common:      │
   │  - One customer → multiple accounts          │
   │  - One account → multiple customers (joint)  │
   │                                              │
   │  Bridge benefits:                            │
   │  - Pre-computed joins (fast queries)         │
   │  - Denormalized attributes from satellites   │
   │  - Business logic applied once               │
   │  - No complex joins at query time            │
   └──────────────────────────────────────────────┘


┌───────────────────────────────────────────────────┐
│     REF_PRODUCT_HIERARCHY                         │   ◄────Reference Data
│───────────────────────────────────────────────────│
│ PK product_hierarchy_key (VARCHAR)                │
│    hub_product_hk (VARCHAR)                       │
│    product_code (VARCHAR)                         │
│    product_name (VARCHAR)                         │
│    product_type (VARCHAR)                         │
│    product_category (VARCHAR)                     │
│    product_family (VARCHAR)                       │   ◄────Derived hierarchy
│    product_division (VARCHAR)                     │   ◄────Derived hierarchy
│    is_deposit_product (BOOLEAN)                   │   ◄────Business rule
│    is_lending_product (BOOLEAN)                   │   ◄────Business rule
│    risk_category (VARCHAR)                        │   ◄────Business classification
│    load_date (TIMESTAMP)                          │
└───────────────────────────────────────────────────┘


┌───────────────────────────────────────────────────┐
│     REF_TRANSACTION_CATEGORY_HIERARCHY            │
│───────────────────────────────────────────────────│
│ PK category_hierarchy_key (VARCHAR)               │
│    hub_category_hk (VARCHAR)                      │
│    category_code (VARCHAR)                        │
│    category_name (VARCHAR)                        │
│    parent_category_code (VARCHAR)                 │
│    parent_category_name (VARCHAR)                 │
│    category_level (INT)                           │   ◄────1=parent, 2=child
│    category_path (VARCHAR)                        │   ◄────Shopping/Groceries
│    is_expense_category (BOOLEAN)                  │
│    is_income_category (BOOLEAN)                   │
│    load_date (TIMESTAMP)                          │
└───────────────────────────────────────────────────┘
```

### Business Vault Queries

#### Query 1: Customer Profile as of Specific Date
```sql
-- Get customer details as they existed on 2025-01-15
SELECT * FROM pit_customer
WHERE hub_customer_hk = 'A3F5E8C9D1B2...'
  AND snapshot_date = '2025-01-15';
```

#### Query 2: All Accounts for Customer
```sql
-- Get all accounts with current balances
SELECT 
    customer_number,
    account_number,
    product_name,
    current_balance,
    account_status
FROM bridge_customer_accounts
WHERE hub_customer_hk = 'A3F5E8C9D1B2...'
  AND is_active_account = TRUE;
```

---

## Model 4: Dimensional Model (Gold Layer - Star Schema)

### Purpose
The **Dimensional Model** provides business-friendly star schema for BI tools and analytics:
- **Fact tables**: Measurable business events (transactions, balances)
- **Dimension tables**: Descriptive contexts (who, what, where, when, why)
- **Slowly Changing Dimensions**: Type 2 SCD for historical analysis
- **Conformed dimensions**: Reusable across multiple fact tables

### Key Characteristics
- **Denormalized** for query performance
- **Business-friendly names** and structures
- **Pre-calculated metrics** and aggregations
- **Optimized for BI tools** (Tableau, Power BI, etc.)

### ERD: Dimensional Model

```
                    ┌────────────────────────────┐
                    │      DIM_DATE              │   ◄────Time Dimension
                    │────────────────────────────│
                    │ PK date_sk (BIGINT)        │   ◄────Surrogate Key
                    │    date_key (DATE)         │   ◄────Natural Key
                    │    day_of_week (VARCHAR)   │
                    │    day_of_month (INT)      │
                    │    week_of_year (INT)      │
                    │    month (INT)             │
                    │    month_name (VARCHAR)    │
                    │    quarter (INT)           │
                    │    year (INT)              │
                    │    year_month (VARCHAR)    │   ◄────2025-01
                    │    year_quarter (VARCHAR)  │   ◄────2025-Q1
                    │    is_weekend (BOOLEAN)    │
                    │    is_holiday (BOOLEAN)    │
                    │    fiscal_year (INT)       │
                    │    fiscal_quarter (INT)    │
                    └────────────────────────────┘
                              │
                              │
        ┌─────────────────────┼─────────────────────┐
        │                     │                     │
        │                     │                     │
┌───────▼──────────┐   ┌──────▼─────────┐   ┌──────▼─────────┐
│  DIM_CUSTOMER    │   │  DIM_ACCOUNT   │   │  DIM_PRODUCT   │
│──────────────────│   │────────────────│   │────────────────│
│ PK customer_sk   │   │ PK account_sk  │   │ PK product_sk  │
│    customer_bk   │   │    account_bk  │   │    product_bk  │
│    first_name    │   │    account_num │   │    product_code│
│    last_name     │   │ FK customer_sk │   │    product_name│
│    full_name     │   │ FK product_sk  │   │    product_cat │
│    email         │   │ FK branch_sk   │   │    product_type│
│    phone         │   │    acct_status │   │    interest_rt │
│    dob           │   │    currency    │   │    min_balance │
│    age           │   │    overdraft   │   │    monthly_fee │
│    age_group     │   │    opened_date │   │    description │
│    credit_score  │   │    closed_date │   │    is_active   │
│    credit_tier   │   │    is_active   │   └────────────────┘
│    loyalty_tier  │   └────────────────┘            │
│    cust_type     │            │                    │
│    is_business   │            │                    │
│    effective_dt  │◄───Type 2 SCD                   │
│    expiry_date   │            │                    │
│    is_current    │            │                    │
└──────────────────┘            │                    │
        │                       │                    │
        │                       │                    │
        └───────────┬───────────┴────────────────────┘
                    │
                    │
        ┌───────────▼───────────────────────────────────────┐
        │           FACT_TRANSACTIONS                        │   ◄────Transaction Fact
        │────────────────────────────────────────────────────│
        │ PK transaction_sk (BIGINT)                         │
        │ FK customer_sk (BIGINT)                            │
        │ FK account_sk (BIGINT)                             │
        │ FK product_sk (BIGINT)                             │
        │ FK branch_sk (BIGINT)                              │
        │ FK date_sk (BIGINT)                                │
        │ FK time_sk (BIGINT)                                │
        │    transaction_bk (VARCHAR)                        │   ◄────Business Key
        │    transaction_number (VARCHAR)                    │
        │    transaction_type (VARCHAR)                      │
        │    transaction_status (VARCHAR)                    │
        │    channel (VARCHAR)                               │
        │    location (VARCHAR)                              │
        │    -- Measures (Additive)                          │
        │    transaction_amount (DECIMAL)                    │   ◄────Measure
        │    item_count (INT)                                │   ◄────Measure
        │    -- Measures (Semi-Additive)                     │
        │    account_balance_after (DECIMAL)                 │
        │    -- Degenerate Dimensions                        │
        │    reference_number (VARCHAR)                      │
        │    description (TEXT)                              │
        │    -- Timestamps                                   │
        │    transaction_datetime (TIMESTAMP)                │
        │    posting_datetime (TIMESTAMP)                    │
        └────────────────────────────────────────────────────┘
                              │
                              │
                              │ 1
                              │
                              │ *
        ┌─────────────────────▼──────────────────────────────┐
        │        FACT_TRANSACTION_ITEMS                       │   ◄────Item Fact
        │─────────────────────────────────────────────────────│
        │ PK transaction_item_sk (BIGINT)                     │
        │ FK transaction_sk (BIGINT)                          │   ◄────Parent transaction
        │ FK customer_sk (BIGINT)                             │
        │ FK account_sk (BIGINT)                              │
        │ FK category_sk (BIGINT)                             │
        │ FK date_sk (BIGINT)                                 │
        │    item_sequence (INT)                              │
        │    -- Measures                                      │
        │    item_amount (DECIMAL)                            │   ◄────Measure
        │    -- Dimensions                                    │
        │    item_description (TEXT)                          │
        │    payee_name (VARCHAR)                             │
        │    merchant_name (VARCHAR)                          │
        │    merchant_category_code (VARCHAR)                 │
        │    is_recurring (BOOLEAN)                           │
        └─────────────────────────────────────────────────────┘


┌────────────────────────────┐         ┌────────────────────────────┐
│      DIM_BRANCH            │         │     DIM_CATEGORY           │
│────────────────────────────│         │────────────────────────────│
│ PK branch_sk (BIGINT)      │         │ PK category_sk (BIGINT)    │
│    branch_bk (VARCHAR)     │         │    category_bk (VARCHAR)   │
│    branch_code (VARCHAR)   │         │    category_code (VARCHAR) │
│    branch_name (VARCHAR)   │         │    category_name (VARCHAR) │
│    branch_type (VARCHAR)   │         │    parent_category (VAR)   │
│    address_line1 (VARCHAR) │         │    category_level (INT)    │
│    address_line2 (VARCHAR) │         │    category_path (VARCHAR) │
│    city (VARCHAR)          │         │    is_expense (BOOLEAN)    │
│    state (VARCHAR)         │         │    is_income (BOOLEAN)     │
│    zip_code (VARCHAR)      │         │    description (TEXT)      │
│    region (VARCHAR)        │         └────────────────────────────┘
│    manager_name (VARCHAR)  │
│    opening_date (DATE)     │
│    is_active (BOOLEAN)     │
└────────────────────────────┘


        ┌──────────────────────────────────────────┐
        │   FACT_ACCOUNT_BALANCE_DAILY             │   ◄────Periodic Snapshot
        │──────────────────────────────────────────│
        │ PK account_balance_sk (BIGINT)           │
        │ FK account_sk (BIGINT)                   │
        │ FK customer_sk (BIGINT)                  │
        │ FK product_sk (BIGINT)                   │
        │ FK branch_sk (BIGINT)                    │
        │ FK date_sk (BIGINT)                      │
        │    -- Semi-Additive Measures             │
        │    opening_balance (DECIMAL)             │
        │    closing_balance (DECIMAL)             │   ◄────Balance at end of day
        │    available_balance (DECIMAL)           │
        │    -- Additive Measures                  │
        │    total_deposits (DECIMAL)              │   ◄────Sum of deposits
        │    total_withdrawals (DECIMAL)           │   ◄────Sum of withdrawals
        │    total_fees (DECIMAL)                  │
        │    transaction_count (INT)               │
        │    deposit_count (INT)                   │
        │    withdrawal_count (INT)                │
        └──────────────────────────────────────────┘
```

### Star Schema Benefits

#### Simplified Queries
```sql
-- Without star schema (Raw Vault - complex)
SELECT c.first_name, SUM(ti.item_amount)
FROM hub_customer hc
JOIN sat_customer sc ON hc.hub_customer_hk = sc.hub_customer_hk
JOIN link_customer_account lca ON hc.hub_customer_hk = lca.hub_customer_hk
JOIN hub_account ha ON lca.hub_account_hk = ha.hub_account_hk
JOIN link_account_transaction lat ON ha.hub_account_hk = lat.hub_account_hk
-- ... many more joins ...
GROUP BY c.first_name;

-- With star schema (Dimensional Model - simple)
SELECT c.first_name, SUM(f.item_amount)
FROM fact_transaction_items f
JOIN dim_customer c ON f.customer_sk = c.customer_sk
WHERE c.is_current = TRUE
GROUP BY c.first_name;
```

#### Business-Friendly
- **Meaningful names**: `customer_sk` instead of `hub_customer_hk`
- **Derived attributes**: `age_group`, `credit_tier`
- **Pre-calculated metrics**: `item_count`, `transaction_count`

#### BI Tool Optimization
- **Single fact table** for most queries
- **Conformed dimensions** reused across facts
- **Fast aggregations** (star join optimization)

---

## Schema Evolution: Cross-Layer Impact

### Scenario: Adding `loyalty_tier` to Customer

#### Source System (Model 1)
```sql
ALTER TABLE customer ADD COLUMN loyalty_tier VARCHAR(20);
```

#### Raw Vault (Model 2) - Absorbs Change
```
SAT_CUSTOMER:
- New column added automatically by Spark schema merging
- New satellite records created for modified customers
- hash_diff changes for all affected records
- Hub and Links remain unchanged ✓
```

#### Business Vault (Model 3) - Update PITs
```sql
-- PIT table rebuild includes new column
CREATE OR REPLACE TABLE pit_customer AS
SELECT 
    ...,
    loyalty_tier,  -- New column
    ...
FROM raw_vault...
```

#### Dimensional Model (Model 4) - Controlled Migration
```sql
-- Add to dimension when business is ready
ALTER TABLE dim_customer ADD COLUMN loyalty_tier VARCHAR(20);

-- Populate from PIT
UPDATE dim_customer SET loyalty_tier = pit.loyalty_tier
FROM pit_customer pit
WHERE dim_customer.customer_bk = pit.hub_customer_hk;
```

### Key Insight
**Data Vault prevents schema drift propagation:**
1. Source changes are captured in Raw Vault
2. Business Vault can be selectively updated
3. Dimensional model changes only when business is ready
4. No breaking changes to reports/dashboards

---

## Summary Comparison

| Aspect | Source (3NF) | Raw Vault | Business Vault | Dimensional |
|--------|--------------|-----------|----------------|-------------|
| **Purpose** | Operational | Historical Storage | Analytics Prep | BI/Reporting |
| **Normal Form** | 3NF | Denormalized | Denormalized | Star Schema |
| **Updates** | UPDATE/DELETE | INSERT only | Rebuild | Type 2 SCD |
| **Joins** | Many (complex) | Many (complex) | Moderate | Few (simple) |
| **History** | Current state | Full history | Snapshots | Type 2 history |
| **Schema Change** | ALTER TABLE | Add to SAT | Rebuild PIT | Controlled migration |
| **Query Speed** | Fast (indexed) | Moderate | Fast (pre-joined) | Fastest (star) |
| **Use Case** | Transactions | Audit/Compliance | Analytics | Dashboards |

---

**Next Document**: [Architecture Overview](03_architecture.md) - See how all layers work together

