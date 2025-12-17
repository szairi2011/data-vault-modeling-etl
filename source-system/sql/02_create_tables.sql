-- ============================================
-- Banking Source System Schema (3NF Normalized)
-- ============================================
-- This script creates a normalized banking schema following Third Normal Form (3NF)
-- Models transactions similar to e-commerce orders with multiple items per transaction
-- Run after 01_create_database.sql using: psql -U postgres -d banking_source -f 02_create_tables.sql

SET search_path TO banking;

-- ============================================
-- DIMENSION TABLES (Master Data)
-- ============================================

-- Customer table - stores individual or business customer information
-- Business Key: customer_number (e.g., CUS-000001)
CREATE TABLE customer (
    customer_id SERIAL PRIMARY KEY,
    customer_number VARCHAR(20) UNIQUE NOT NULL, -- Business key for Data Vault
    customer_type VARCHAR(20) NOT NULL, -- INDIVIDUAL or BUSINESS
    first_name VARCHAR(100), -- For individual customers
    last_name VARCHAR(100), -- For individual customers
    business_name VARCHAR(200), -- For business customers
    email VARCHAR(255) UNIQUE,
    phone VARCHAR(20),
    date_of_birth DATE, -- For individuals only
    ssn VARCHAR(11), -- Social Security Number (for individuals)
    tax_id VARCHAR(20), -- Tax ID (for businesses)
    credit_score INTEGER,
    customer_since DATE NOT NULL,
    loyalty_tier VARCHAR(20) DEFAULT 'STANDARD', -- STANDARD, SILVER, GOLD, PLATINUM
    preferred_contact_method VARCHAR(20), -- EMAIL, PHONE, SMS, MAIL
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- CDC tracking
    CONSTRAINT chk_customer_type CHECK (customer_type IN ('INDIVIDUAL', 'BUSINESS'))
);

-- Branch table - physical bank locations
-- Business Key: branch_code (e.g., BR-001)
CREATE TABLE branch (
    branch_id SERIAL PRIMARY KEY,
    branch_code VARCHAR(10) UNIQUE NOT NULL, -- Business key for Data Vault
    branch_name VARCHAR(100) NOT NULL,
    branch_type VARCHAR(20), -- RETAIL, COMMERCIAL, INVESTMENT
    address_line1 VARCHAR(200),
    address_line2 VARCHAR(200),
    city VARCHAR(100),
    state VARCHAR(50),
    zip_code VARCHAR(10),
    country VARCHAR(50) DEFAULT 'USA',
    phone VARCHAR(20),
    manager_name VARCHAR(100),
    opening_date DATE,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Product table - types of banking products offered
-- Business Key: product_code (e.g., PROD-CHK-001)
CREATE TABLE product (
    product_id SERIAL PRIMARY KEY,
    product_code VARCHAR(20) UNIQUE NOT NULL, -- Business key for Data Vault
    product_name VARCHAR(100) NOT NULL,
    product_category VARCHAR(50), -- DEPOSIT, LOAN, INVESTMENT, CARD
    product_type VARCHAR(50), -- CHECKING, SAVINGS, MORTGAGE, CREDIT_CARD, etc.
    interest_rate DECIMAL(5,2), -- Annual percentage rate
    minimum_balance DECIMAL(15,2),
    monthly_fee DECIMAL(10,2),
    overdraft_limit DECIMAL(15,2),
    description TEXT,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP -- CDC tracking
);

-- Transaction category - for expense categorization and reporting
-- Supports hierarchical categories (parent-child relationships)
-- Business Key: category_code (e.g., CAT-FOOD)
CREATE TABLE transaction_category (
    category_id SERIAL PRIMARY KEY,
    category_code VARCHAR(20) UNIQUE NOT NULL, -- Business key for Data Vault
    category_name VARCHAR(100) NOT NULL,
    parent_category_id INTEGER REFERENCES transaction_category(category_id), -- Hierarchical structure
    description TEXT,
    is_active BOOLEAN DEFAULT TRUE
);

-- ============================================
-- FACT TABLES (Transactional Data)
-- ============================================

-- Account table - customer's banking accounts
-- Business Key: account_number (e.g., ACC-123456789)
CREATE TABLE account (
    account_id SERIAL PRIMARY KEY,
    account_number VARCHAR(20) UNIQUE NOT NULL, -- Business key for Data Vault
    customer_id INTEGER NOT NULL REFERENCES customer(customer_id),
    product_id INTEGER NOT NULL REFERENCES product(product_id),
    branch_id INTEGER NOT NULL REFERENCES branch(branch_id),
    account_status VARCHAR(20) NOT NULL, -- ACTIVE, CLOSED, FROZEN, SUSPENDED
    current_balance DECIMAL(15,2) NOT NULL DEFAULT 0.00,
    available_balance DECIMAL(15,2) NOT NULL DEFAULT 0.00, -- Balance minus pending transactions
    currency VARCHAR(3) DEFAULT 'USD',
    overdraft_limit DECIMAL(15,2) DEFAULT 0.00,
    interest_rate DECIMAL(5,2),
    opened_date DATE NOT NULL,
    closed_date DATE,
    last_transaction_date TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- CDC tracking
    CONSTRAINT chk_account_status CHECK (account_status IN ('ACTIVE', 'CLOSED', 'FROZEN', 'SUSPENDED'))
);

-- Transaction header - analogous to order header in e-commerce
-- One transaction can include multiple items (e.g., bill payment with multiple bills)
-- Business Key: transaction_number (e.g., TXN-2025-001234)
CREATE TABLE transaction_header (
    transaction_id SERIAL PRIMARY KEY,
    transaction_number VARCHAR(30) UNIQUE NOT NULL, -- Business key for Data Vault
    account_id INTEGER NOT NULL REFERENCES account(account_id),
    transaction_type VARCHAR(30) NOT NULL, -- DEPOSIT, WITHDRAWAL, TRANSFER, PAYMENT, FEE
    transaction_status VARCHAR(20) NOT NULL, -- PENDING, COMPLETED, FAILED, REVERSED
    total_amount DECIMAL(15,2) NOT NULL, -- Sum of all transaction items
    transaction_date TIMESTAMP NOT NULL,
    posting_date TIMESTAMP, -- When transaction actually posted to account
    channel VARCHAR(20), -- ATM, BRANCH, ONLINE, MOBILE, PHONE
    location VARCHAR(200), -- Where transaction occurred
    description TEXT,
    reference_number VARCHAR(50), -- External reference (check number, wire confirmation, etc.)
    initiated_by VARCHAR(100), -- User or system that initiated transaction
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- CDC tracking
    CONSTRAINT chk_txn_type CHECK (transaction_type IN ('DEPOSIT', 'WITHDRAWAL', 'TRANSFER', 'PAYMENT', 'FEE', 'INTEREST', 'ADJUSTMENT')),
    CONSTRAINT chk_txn_status CHECK (transaction_status IN ('PENDING', 'COMPLETED', 'FAILED', 'REVERSED'))
);

-- Transaction item - analogous to order line item in e-commerce
-- Allows breaking down a transaction into multiple components
-- Example: Bill payment transaction with multiple bills (electricity, water, internet)
-- This is the KEY FEATURE that makes transactions similar to e-commerce orders
CREATE TABLE transaction_item (
    item_id SERIAL PRIMARY KEY,
    transaction_id INTEGER NOT NULL REFERENCES transaction_header(transaction_id),
    item_sequence INTEGER NOT NULL, -- Line number within transaction (1, 2, 3, ...)
    category_id INTEGER REFERENCES transaction_category(category_id),
    item_amount DECIMAL(15,2) NOT NULL,
    item_description TEXT,
    payee_name VARCHAR(200), -- Who received the payment (for PAYMENT transactions)
    payee_account VARCHAR(50), -- Payee account number (for TRANSFER/PAYMENT)
    merchant_name VARCHAR(200), -- Merchant name (for card transactions)
    merchant_category_code VARCHAR(10), -- MCC code for card transactions
    is_recurring BOOLEAN DEFAULT FALSE, -- Is this a recurring payment item?
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_txn_item UNIQUE (transaction_id, item_sequence)
);

-- Transfer details - for TRANSFER transactions (links two accounts)
-- This is a link table connecting sender and receiver accounts
CREATE TABLE transfer_detail (
    transfer_id SERIAL PRIMARY KEY,
    transaction_id INTEGER NOT NULL REFERENCES transaction_header(transaction_id),
    from_account_id INTEGER NOT NULL REFERENCES account(account_id),
    to_account_id INTEGER NOT NULL REFERENCES account(account_id),
    transfer_type VARCHAR(20), -- INTERNAL, WIRE, ACH, P2P
    routing_number VARCHAR(20),
    swift_code VARCHAR(20),
    intermediary_bank VARCHAR(200),
    transfer_fee DECIMAL(10,2) DEFAULT 0.00,
    CONSTRAINT chk_transfer_accounts CHECK (from_account_id <> to_account_id)
);

-- ============================================
-- INDEXES FOR PERFORMANCE
-- ============================================

-- Customer indexes
CREATE INDEX idx_customer_email ON customer(email);
CREATE INDEX idx_customer_type ON customer(customer_type);
CREATE INDEX idx_customer_loyalty ON customer(loyalty_tier);
CREATE INDEX idx_customer_updated ON customer(updated_at); -- For CDC (Change Data Capture)

-- Account indexes
CREATE INDEX idx_account_customer ON account(customer_id);
CREATE INDEX idx_account_product ON account(product_id);
CREATE INDEX idx_account_branch ON account(branch_id);
CREATE INDEX idx_account_status ON account(account_status);
CREATE INDEX idx_account_updated ON account(updated_at); -- For CDC

-- Transaction header indexes
CREATE INDEX idx_txn_account ON transaction_header(account_id);
CREATE INDEX idx_txn_date ON transaction_header(transaction_date);
CREATE INDEX idx_txn_type ON transaction_header(transaction_type);
CREATE INDEX idx_txn_status ON transaction_header(transaction_status);
CREATE INDEX idx_txn_updated ON transaction_header(updated_at); -- For CDC

-- Transaction item indexes (for efficient joins)
CREATE INDEX idx_item_txn ON transaction_item(transaction_id);
CREATE INDEX idx_item_category ON transaction_item(category_id);
CREATE INDEX idx_item_merchant ON transaction_item(merchant_name);

-- Transfer detail indexes
CREATE INDEX idx_transfer_from ON transfer_detail(from_account_id);
CREATE INDEX idx_transfer_to ON transfer_detail(to_account_id);

-- ============================================
-- TRIGGERS FOR AUTOMATIC TIMESTAMP UPDATES
-- ============================================

-- Function to automatically update updated_at column on row modification
-- This is crucial for CDC (Change Data Capture) in Data Vault
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Apply trigger to all tables with updated_at column
CREATE TRIGGER update_customer_updated_at BEFORE UPDATE ON customer
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_product_updated_at BEFORE UPDATE ON product
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_account_updated_at BEFORE UPDATE ON account
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_transaction_updated_at BEFORE UPDATE ON transaction_header
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- ============================================
-- TABLE COMMENTS FOR DOCUMENTATION
-- ============================================

COMMENT ON TABLE customer IS 'Customer master data - individuals and businesses';
COMMENT ON TABLE account IS 'Customer accounts - checking, savings, loans, cards';
COMMENT ON TABLE transaction_header IS 'Transaction header - analogous to order in e-commerce, one-to-many with items';
COMMENT ON TABLE transaction_item IS 'Transaction line items - multiple items per transaction (e.g., multiple bills paid in one transaction)';
COMMENT ON TABLE transfer_detail IS 'Transfer relationships - links sender and receiver accounts';

COMMENT ON COLUMN customer.customer_number IS 'Business key for Data Vault Hub';
COMMENT ON COLUMN customer.updated_at IS 'Timestamp for CDC - tracks when record was last modified';
COMMENT ON COLUMN transaction_header.transaction_number IS 'Business key for Data Vault Hub';
COMMENT ON COLUMN transaction_item.item_sequence IS 'Line number within parent transaction - allows multiple items per transaction';

-- Display confirmation
SELECT 'Banking source system schema created successfully!' as status;
SELECT 'Tables created: customer, branch, product, transaction_category, account, transaction_header, transaction_item, transfer_detail' as tables;

