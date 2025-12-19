package seeder

import java.sql.{Connection, DriverManager}

/**
 * Reference Data Seeder for Banking Source System
 *
 * This object seeds reference/master data that changes infrequently:
 * - Transaction categories (hierarchical: parent and child categories)
 * - Bank branches (physical locations)
 * - Banking products (account types, loans, credit cards)
 *
 * This data is relatively static and serves as lookup tables for transactional data.
 *
 * Usage:
 * sbt "runMain seeder.ReferenceDataSeeder"
 */
object ReferenceDataSeeder {

  // JDBC connection parameters for PostgreSQL source system
  val jdbcUrl = "jdbc:postgresql://localhost:5432/banking_source"
  val user = "postgres"
//  val password = "postgres"
  val password = "passw0rd"

  def main(args: Array[String]): Unit = {
    println("=" * 60)
    println("Starting Reference Data Seeding")
    println("=" * 60)

    var conn:Connection = null

    try {
      // Create JDBC connection for Scala 2.12
      conn = DriverManager.getConnection(jdbcUrl, user, password)
      conn.setAutoCommit(false) // Enable transaction management

      // Seed in dependency order (categories first, then others)
      seedTransactionCategories(conn)
      seedBranches(conn)
      seedProducts(conn)

      conn.commit()
      println("\n" + "=" * 60)
      println("Reference data seeding completed successfully!")
      println("=" * 60)

    } catch {
      case e: Exception =>
        conn.rollback()
        println(s"\nERROR: Failed to seed data: ${e.getMessage}")
        e.printStackTrace()
        throw e
    }
  }

  /**
   * Seed transaction categories with hierarchical structure
   *
   * Categories follow a parent-child relationship pattern:
   *   - Shopping (parent)
   *     ├── Groceries (child)
   *     └── Clothing (child)
   *   - Food & Dining (parent)
   *     ├── Restaurants (child)
   *     └── Fast Food (child)
   *
   * This hierarchical structure allows for flexible reporting and analysis
   * at both summary and detailed levels.
   */
  def seedTransactionCategories(conn: Connection): Unit = {
    println("\n[1/3] Seeding transaction categories...")

    val stmt = conn.prepareStatement(
      "INSERT INTO banking.transaction_category (category_code, category_name, parent_category_id, description) VALUES (?, ?, ?, ?)"
    )

    // STEP 1: Insert parent categories (top-level categories)
    val parentCategories = Seq(
      ("CAT-SHOPPING", "Shopping", "General shopping expenses"),
      ("CAT-UTILITIES", "Utilities", "Utility bills and services"),
      ("CAT-TRANSPORT", "Transportation", "Travel and transportation"),
      ("CAT-FOOD", "Food & Dining", "Restaurants and groceries"),
      ("CAT-HEALTH", "Healthcare", "Medical and health expenses"),
      ("CAT-ENTERTAIN", "Entertainment", "Entertainment and leisure"),
      ("CAT-INCOME", "Income", "Salary and other income"),
      ("CAT-TRANSFER", "Transfers", "Account transfers")
    )

    parentCategories.foreach { case (code, name, desc) =>
      stmt.setString(1, code)
      stmt.setString(2, name)
      stmt.setNull(3, java.sql.Types.INTEGER) // No parent for top-level categories
      stmt.setString(4, desc)
      stmt.executeUpdate()
    }

    // STEP 2: Retrieve parent category IDs for creating child relationships
    val parentIdMap = collection.mutable.Map[String, Int]()
    val rs = conn.createStatement().executeQuery(
      "SELECT category_id, category_code FROM banking.transaction_category WHERE parent_category_id IS NULL"
    )
    while (rs.next()) {
      parentIdMap(rs.getString("category_code")) = rs.getInt("category_id")
    }

    // STEP 3: Insert child categories (sub-categories)
    val childCategories = Seq(
      ("CAT-GROCERY", "Groceries", "CAT-FOOD", "Supermarket purchases"),
      ("CAT-RESTAURANT", "Restaurants", "CAT-FOOD", "Dining out"),
      ("CAT-ELECTRIC", "Electricity", "CAT-UTILITIES", "Electric utility bill"),
      ("CAT-WATER", "Water", "CAT-UTILITIES", "Water utility bill"),
      ("CAT-INTERNET", "Internet", "CAT-UTILITIES", "Internet service"),
      ("CAT-GAS", "Gas & Fuel", "CAT-TRANSPORT", "Vehicle fuel"),
      ("CAT-PARKING", "Parking", "CAT-TRANSPORT", "Parking fees"),
      ("CAT-PHARMACY", "Pharmacy", "CAT-HEALTH", "Prescription medications"),
      ("CAT-MOVIES", "Movies", "CAT-ENTERTAIN", "Cinema tickets"),
      ("CAT-SALARY", "Salary", "CAT-INCOME", "Employment income"),
      ("CAT-INTERNAL", "Internal Transfer", "CAT-TRANSFER", "Transfer between own accounts")
    )

    childCategories.foreach { case (code, name, parentCode, desc) =>
      stmt.setString(1, code)
      stmt.setString(2, name)
      stmt.setInt(3, parentIdMap(parentCode)) // Link to parent category
      stmt.setString(4, desc)
      stmt.executeUpdate()
    }

    println(s"  ✓ Inserted ${parentCategories.size} parent categories")
    println(s"  ✓ Inserted ${childCategories.size} child categories")
    println(s"  ✓ Total: ${parentCategories.size + childCategories.size} categories")
  }

  /**
   * Seed bank branch locations
   *
   * Creates branches in different cities across the United States.
   * Each branch has a type (RETAIL, COMMERCIAL, INVESTMENT) which determines
   * the services it offers and the types of customers it serves.
   */
  def seedBranches(conn: Connection): Unit = {
    println("\n[2/3] Seeding bank branches...")

    val stmt = conn.prepareStatement(
      """INSERT INTO banking.branch
         (branch_code, branch_name, branch_type, address_line1, city, state, zip_code, phone, manager_name, opening_date)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
    )

    // Define branches across major US cities
    // Mix of RETAIL (consumer banking), COMMERCIAL (business banking), and INVESTMENT branches
    val branches = Seq(
      ("BR-001", "Main Street Branch", "RETAIL", "100 Main St", "New York", "NY", "10001", "212-555-0101", "John Smith", "2010-01-15"),
      ("BR-002", "Downtown Commercial", "COMMERCIAL", "250 Broadway", "New York", "NY", "10007", "212-555-0102", "Sarah Johnson", "2012-03-20"),
      ("BR-003", "Hollywood Branch", "RETAIL", "500 Hollywood Blvd", "Los Angeles", "CA", "90028", "323-555-0103", "Michael Chen", "2011-06-10"),
      ("BR-004", "Chicago Loop", "RETAIL", "75 State St", "Chicago", "IL", "60602", "312-555-0104", "Emily Davis", "2013-09-05"),
      ("BR-005", "Houston Energy Center", "COMMERCIAL", "1000 Louisiana St", "Houston", "TX", "77002", "713-555-0105", "Robert Martinez", "2014-11-12"),
      ("BR-006", "Phoenix Desert View", "RETAIL", "200 Central Ave", "Phoenix", "AZ", "85004", "602-555-0106", "Jennifer Wilson", "2015-02-28"),
      ("BR-007", "Manhattan Investment", "INVESTMENT", "375 Park Ave", "New York", "NY", "10152", "212-555-0107", "David Brown", "2016-04-15"),
      ("BR-008", "San Francisco Tech", "COMMERCIAL", "50 California St", "San Francisco", "CA", "94111", "415-555-0108", "Lisa Anderson", "2017-07-20"),
      ("BR-009", "Boston Harbor", "RETAIL", "100 Federal St", "Boston", "MA", "02110", "617-555-0109", "James Taylor", "2018-10-01"),
      ("BR-010", "Seattle Waterfront", "RETAIL", "400 Pine St", "Seattle", "WA", "98101", "206-555-0110", "Amanda White", "2019-12-15")
    )

    branches.foreach { case (code, name, bType, address, city, state, zip, phone, manager, openDate) =>
      stmt.setString(1, code)
      stmt.setString(2, name)
      stmt.setString(3, bType)
      stmt.setString(4, address)
      stmt.setString(5, city)
      stmt.setString(6, state)
      stmt.setString(7, zip)
      stmt.setString(8, phone)
      stmt.setString(9, manager)
      stmt.setDate(10, java.sql.Date.valueOf(openDate))
      stmt.executeUpdate()
    }

    println(s"  ✓ Inserted ${branches.size} branches")
    println(s"    - RETAIL branches: ${branches.count(_._3 == "RETAIL")}")
    println(s"    - COMMERCIAL branches: ${branches.count(_._3 == "COMMERCIAL")}")
    println(s"    - INVESTMENT branches: ${branches.count(_._3 == "INVESTMENT")}")
  }

  /**
   * Seed banking products
   *
   * Products represent different account types and financial services:
   * - DEPOSIT: Checking and savings accounts
   * - LOAN: Personal loans, auto loans, mortgages
   * - CARD: Credit and debit cards
   *
   * Each product has specific terms (interest rates, fees, limits) that
   * differentiate it from other products in the same category.
   */
  def seedProducts(conn: Connection): Unit = {
    println("\n[3/3] Seeding banking products...")

    val stmt = conn.prepareStatement(
      """INSERT INTO banking.product
         (product_code, product_name, product_category, product_type, interest_rate, minimum_balance, monthly_fee, overdraft_limit, description)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"""
    )

    val products = Seq(
      // Checking accounts - for everyday transactions
      ("PROD-CHK-001", "Basic Checking", "DEPOSIT", "CHECKING", 0.00, 0.00, 5.00, 100.00, "No-frills checking account for everyday banking"),
      ("PROD-CHK-002", "Premium Checking", "DEPOSIT", "CHECKING", 0.10, 1000.00, 0.00, 500.00, "Premium checking with higher interest and no monthly fee"),
      ("PROD-CHK-003", "Business Checking", "DEPOSIT", "CHECKING", 0.05, 500.00, 15.00, 1000.00, "Checking account for small businesses"),

      // Savings accounts - for saving and earning interest
      ("PROD-SAV-001", "Basic Savings", "DEPOSIT", "SAVINGS", 0.50, 100.00, 0.00, 0.00, "Entry-level savings account"),
      ("PROD-SAV-002", "High Yield Savings", "DEPOSIT", "SAVINGS", 2.50, 5000.00, 0.00, 0.00, "High interest savings for larger balances"),
      ("PROD-SAV-003", "Money Market", "DEPOSIT", "MONEY_MARKET", 3.00, 10000.00, 10.00, 0.00, "Money market account with check-writing privileges"),

      // Loans - borrowing products
      ("PROD-LOAN-001", "Personal Loan", "LOAN", "PERSONAL", 8.99, 0.00, 0.00, 0.00, "Unsecured personal loan"),
      ("PROD-LOAN-002", "Auto Loan", "LOAN", "AUTO", 5.49, 0.00, 0.00, 0.00, "New and used vehicle financing"),
      ("PROD-LOAN-003", "Mortgage 30-Year", "LOAN", "MORTGAGE", 6.75, 0.00, 0.00, 0.00, "30-year fixed rate mortgage"),

      // Credit Cards - revolving credit
      ("PROD-CARD-001", "Standard Credit Card", "CARD", "CREDIT_CARD", 18.99, 0.00, 0.00, 5000.00, "Basic credit card with rewards"),
      ("PROD-CARD-002", "Gold Credit Card", "CARD", "CREDIT_CARD", 15.99, 0.00, 95.00, 15000.00, "Premium credit card with travel benefits"),
      ("PROD-CARD-003", "Business Credit Card", "CARD", "CREDIT_CARD", 16.99, 0.00, 0.00, 25000.00, "Credit card for business expenses")
    )

    products.foreach { case (code, name, category, pType, rate, minBal, fee, overdraft, desc) =>
      stmt.setString(1, code)
      stmt.setString(2, name)
      stmt.setString(3, category)
      stmt.setString(4, pType)
      stmt.setBigDecimal(5, BigDecimal(rate).bigDecimal)
      stmt.setBigDecimal(6, BigDecimal(minBal).bigDecimal)
      stmt.setBigDecimal(7, BigDecimal(fee).bigDecimal)
      stmt.setBigDecimal(8, BigDecimal(overdraft).bigDecimal)
      stmt.setString(9, desc)
      stmt.executeUpdate()
    }

    println(s"  ✓ Inserted ${products.size} products")
    println(s"    - DEPOSIT products: ${products.count(_._3 == "DEPOSIT")}")
    println(s"    - LOAN products: ${products.count(_._3 == "LOAN")}")
    println(s"    - CARD products: ${products.count(_._3 == "CARD")}")
  }
}

