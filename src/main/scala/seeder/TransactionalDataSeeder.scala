package seeder

import java.sql.{Connection, DriverManager, Timestamp}
import scala.util.{Random, Using}
import java.time.{LocalDateTime, LocalDate}

/**
 * Transactional Data Seeder for Banking Source System
 * 
 * This object seeds transactional data that changes frequently:
 * - Customers (individuals and businesses)
 * - Accounts (checking, savings, loans, credit cards)
 * - Transactions with multiple items (like e-commerce orders)
 * 
 * KEY FEATURE: Transactions can have multiple items, similar to how an e-commerce order
 * has multiple line items. For example:
 *   - A bill payment transaction might pay electricity, water, and internet (3 items)
 *   - A shopping withdrawal might include groceries, pharmacy, and gas (3 items)
 * 
 * This design demonstrates how Data Vault handles one-to-many relationships effectively.
 * 
 * Usage:
 *   sbt "runMain seeder.TransactionalDataSeeder"
 */
object TransactionalDataSeeder {

  // JDBC connection parameters for PostgreSQL source system
  val jdbcUrl = "jdbc:postgresql://localhost:5432/banking_source"
  val user = "postgres"
  val password = "postgres"
  val random = new Random(42) // Fixed seed for reproducibility in testing

  def main(args: Array[String]): Unit = {
    println("=" * 60)
    println("Starting Transactional Data Seeding")
    println("=" * 60)

    Using(DriverManager.getConnection(jdbcUrl, user, password)) { conn =>
      conn.setAutoCommit(false) // Use transactions for data integrity

      try {
        // STEP 1: Get reference data IDs (branches, products, categories)
        println("\n[Step 1/4] Loading reference data...")
        val branchIds = getBranchIds(conn)
        val productIds = getProductIds(conn)
        val categoryIds = getCategoryIds(conn)
        println(s"  ✓ Loaded ${branchIds.size} branches, ${productIds.values.map(_.size).sum} products, ${categoryIds.size} categories")

        // STEP 2: Seed customers (1000 customers: 90% individual, 10% business)
        println("\n[Step 2/4] Seeding customers...")
        val customerIds = seedCustomers(conn, 1000)

        // STEP 3: Seed accounts (each customer gets 1-3 accounts)
        println("\n[Step 3/4] Seeding accounts...")
        val accountIds = seedAccounts(conn, customerIds, branchIds, productIds)

        // STEP 4: Seed transactions with multiple items (5000 transactions)
        println("\n[Step 4/4] Seeding transactions with items...")
        seedTransactionsWithItems(conn, accountIds, categoryIds, 5000)

        conn.commit()
        println("\n" + "=" * 60)
        println("Transactional data seeding completed successfully!")
        println("=" * 60)

      } catch {
        case e: Exception =>
          conn.rollback()
          println(s"\nERROR: Failed to seed data: ${e.getMessage}")
          e.printStackTrace()
          throw e
      }
    }.get
  }

  /**
   * Retrieve all branch IDs from the database
   * These will be randomly assigned to accounts
   */
  def getBranchIds(conn: Connection): Seq[Int] = {
    val rs = conn.createStatement().executeQuery("SELECT branch_id FROM banking.branch")
    Iterator.continually(rs).takeWhile(_.next()).map(_.getInt("branch_id")).toSeq
  }

  /**
   * Retrieve product IDs grouped by category
   * Returns: Map("DEPOSIT" -> [1,2,3], "LOAN" -> [4,5], "CARD" -> [6,7])
   * This allows us to assign appropriate products to accounts based on type
   */
  def getProductIds(conn: Connection): Map[String, Seq[Int]] = {
    val rs = conn.createStatement().executeQuery("SELECT product_id, product_category FROM banking.product")
    Iterator.continually(rs).takeWhile(_.next())
      .map(rs => (rs.getString("product_category"), rs.getInt("product_id")))
      .toSeq
      .groupBy(_._1)
      .view.mapValues(_.map(_._2)).toMap
  }

  /**
   * Retrieve category IDs mapped by their codes
   * Returns: Map("CAT-GROCERY" -> 5, "CAT-ELECTRIC" -> 7, ...)
   * Used to assign categories to transaction items
   */
  def getCategoryIds(conn: Connection): Map[String, Int] = {
    val rs = conn.createStatement().executeQuery("SELECT category_id, category_code FROM banking.transaction_category")
    Iterator.continually(rs).takeWhile(_.next())
      .map(rs => (rs.getString("category_code"), rs.getInt("category_id")))
      .toMap
  }

  /**
   * Seed customer records with realistic attributes
   * 
   * Generates both individual and business customers with appropriate attributes:
   * - Individuals: first_name, last_name, SSN, date_of_birth
   * - Businesses: business_name, tax_id
   * 
   * Each customer is assigned:
   * - Loyalty tier (STANDARD, SILVER, GOLD, PLATINUM)
   * - Credit score (300-850 for individuals, 400-850 for businesses)
   * - Preferred contact method (EMAIL, PHONE, SMS, MAIL)
   * 
   * @param conn Database connection
   * @param count Number of customers to create
   * @return Sequence of created customer IDs
   */
  def seedCustomers(conn: Connection, count: Int): Seq[Int] = {
    val stmt = conn.prepareStatement(
      """INSERT INTO banking.customer 
         (customer_number, customer_type, first_name, last_name, business_name, email, phone, 
          date_of_birth, ssn, tax_id, credit_score, customer_since, loyalty_tier, preferred_contact_method)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
         RETURNING customer_id"""
    )

    // Sample data for generating realistic names
    val firstNames = Array("James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael", "Linda", 
                           "William", "Barbara", "David", "Elizabeth", "Richard", "Susan", "Joseph", "Jessica",
                           "Thomas", "Sarah", "Charles", "Karen", "Christopher", "Nancy", "Daniel", "Lisa")
    val lastNames = Array("Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
                          "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson", "Thomas",
                          "Taylor", "Moore", "Jackson", "Martin", "Lee", "Thompson", "White", "Harris")
    val loyaltyTiers = Array("STANDARD", "SILVER", "GOLD", "PLATINUM")
    val contactMethods = Array("EMAIL", "PHONE", "SMS", "MAIL")

    val customerIds = (1 to count).map { i =>
      // 90% individual customers, 10% business customers (realistic distribution)
      val isIndividual = random.nextInt(100) < 90

      stmt.setString(1, f"CUS-$i%06d") // Business key: CUS-000001
      stmt.setString(2, if (isIndividual) "INDIVIDUAL" else "BUSINESS")

      if (isIndividual) {
        // Individual customer attributes
        stmt.setString(3, firstNames(random.nextInt(firstNames.length)))
        stmt.setString(4, lastNames(random.nextInt(lastNames.length)))
        stmt.setNull(5, java.sql.Types.VARCHAR) // No business name
        stmt.setString(6, s"customer$i@email.com")
        stmt.setString(7, f"555${random.nextInt(10000000)}%07d")
        // Age between 18 and 70 years old
        stmt.setDate(8, java.sql.Date.valueOf(
          LocalDate.now().minusYears(18 + random.nextInt(52))
        ))
        stmt.setString(9, f"${random.nextInt(1000000000)}%09d") // SSN (9 digits)
        stmt.setNull(10, java.sql.Types.VARCHAR) // No tax ID
        stmt.setInt(11, 300 + random.nextInt(551)) // Credit score 300-850
      } else {
        // Business customer attributes
        stmt.setNull(3, java.sql.Types.VARCHAR) // No first name
        stmt.setNull(4, java.sql.Types.VARCHAR) // No last name
        stmt.setString(5, s"${lastNames(random.nextInt(lastNames.length))} Corp")
        stmt.setString(6, s"business$i@company.com")
        stmt.setString(7, f"555${random.nextInt(10000000)}%07d")
        stmt.setNull(8, java.sql.Types.DATE) // No DOB for business
        stmt.setNull(9, java.sql.Types.VARCHAR) // No SSN
        stmt.setString(10, f"${random.nextInt(100000000)}%08d") // Tax ID (8 digits)
        stmt.setInt(11, 400 + random.nextInt(451)) // Business credit score 400-850
      }

      // Common attributes for all customers
      stmt.setDate(12, java.sql.Date.valueOf(
        LocalDate.now().minusYears(1 + random.nextInt(10)) // Customer for 1-10 years
      ))
      stmt.setString(13, loyaltyTiers(random.nextInt(loyaltyTiers.length)))
      stmt.setString(14, contactMethods(random.nextInt(contactMethods.length)))

      val rs = stmt.executeQuery()
      rs.next()
      val customerId = rs.getInt("customer_id")
      
      // Progress indicator every 200 customers
      if (i % 200 == 0) println(s"  ✓ Created $i customers...")
      
      customerId
    }

    println(s"  ✓ Total customers created: ${customerIds.size}")
    println(s"    - Individual customers: ~${(count * 0.9).toInt}")
    println(s"    - Business customers: ~${(count * 0.1).toInt}")
    customerIds
  }

  /**
   * Seed account records
   * 
   * Each customer receives 1-3 accounts with varying product types:
   * - 50% DEPOSIT accounts (checking/savings)
   * - 30% CARD accounts (credit cards)
   * - 20% LOAN accounts (personal loans, mortgages)
   * 
   * Account balances are generated realistically:
   * - DEPOSIT: $100 - $50,000 (positive balance)
   * - CARD: $0 - $5,000 (negative = amount owed)
   * - LOAN: $10,000 - $200,000 (negative = amount owed)
   * 
   * @param conn Database connection
   * @param customerIds List of customer IDs to create accounts for
   * @param branchIds List of branch IDs to assign accounts to
   * @param productIds Map of product category to product IDs
   * @return Sequence of created account IDs
   */
  def seedAccounts(conn: Connection, customerIds: Seq[Int], branchIds: Seq[Int], 
                   productIds: Map[String, Seq[Int]]): Seq[Int] = {
    val stmt = conn.prepareStatement(
      """INSERT INTO banking.account 
         (account_number, customer_id, product_id, branch_id, account_status, 
          current_balance, available_balance, opened_date)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?)
         RETURNING account_id"""
    )

    val statuses = Array("ACTIVE", "ACTIVE", "ACTIVE", "ACTIVE", "CLOSED") // 80% active, 20% closed
    val accountIds = collection.mutable.ArrayBuffer[Int]()
    var accountCounter = 1

    customerIds.foreach { customerId =>
      // Each customer gets 1-3 accounts
      val numAccounts = 1 + random.nextInt(3)

      (1 to numAccounts).foreach { _ =>
        // Distribute accounts across product types realistically
        val productCategory = random.nextInt(10) match {
          case n if n < 5 => "DEPOSIT" // 50% checking/savings
          case n if n < 8 => "CARD"    // 30% credit cards
          case _          => "LOAN"    // 20% loans
        }

        val products = productIds(productCategory)
        val productId = products(random.nextInt(products.size))
        val branchId = branchIds(random.nextInt(branchIds.size))
        val status = statuses(random.nextInt(statuses.length))
        
        // Generate realistic balance based on product type
        val balance = productCategory match {
          case "DEPOSIT" => // Positive balance for deposits
            BigDecimal(100 + random.nextDouble() * 49900).setScale(2, BigDecimal.RoundingMode.HALF_UP)
          case "CARD" => // Negative balance for credit cards (amount owed)
            BigDecimal(-random.nextDouble() * 5000).setScale(2, BigDecimal.RoundingMode.HALF_UP)
          case "LOAN" => // Negative balance for loans (principal owed)
            BigDecimal(-10000 - random.nextDouble() * 190000).setScale(2, BigDecimal.RoundingMode.HALF_UP)
        }

        stmt.setString(1, f"ACC-$accountCounter%09d") // Business key: ACC-000000001
        stmt.setInt(2, customerId)
        stmt.setInt(3, productId)
        stmt.setInt(4, branchId)
        stmt.setString(5, status)
        stmt.setBigDecimal(6, balance.bigDecimal)
        stmt.setBigDecimal(7, balance.bigDecimal) // Available = current for simplicity
        stmt.setDate(8, java.sql.Date.valueOf(
          LocalDate.now().minusYears(random.nextInt(5)) // Account opened 0-5 years ago
        ))

        val rs = stmt.executeQuery()
        rs.next()
        accountIds += rs.getInt("account_id")
        accountCounter += 1
      }
      
      // Progress indicator
      if (accountIds.size % 500 == 0) println(s"  ✓ Created ${accountIds.size} accounts...")
    }

    println(s"  ✓ Total accounts created: ${accountIds.size}")
    accountIds.toSeq
  }

  /**
   * Seed transactions with multiple items (E-COMMERCE PATTERN)
   * 
   * This is the KEY FEATURE that demonstrates multi-item transactions:
   * 
   * Example 1 - Bill Payment Transaction:
   *   Transaction Header: TXN-2025-000123, Type=PAYMENT, Total=$250.00
   *   Items:
   *     1. Electricity bill - $100.00
   *     2. Water bill - $50.00
   *     3. Internet bill - $100.00
   * 
   * Example 2 - Shopping Withdrawal:
   *   Transaction Header: TXN-2025-000124, Type=WITHDRAWAL, Total=$175.50
   *   Items:
   *     1. Groceries at Whole Foods - $120.00
   *     2. Pharmacy at CVS - $30.50
   *     3. Gas at Shell - $25.00
   * 
   * This design mirrors e-commerce order structures and demonstrates how
   * Data Vault handles one-to-many relationships effectively.
   * 
   * Distribution:
   * - PAYMENT transactions: 1-5 items (multiple bills)
   * - WITHDRAWAL transactions: 1-3 items (multiple purchases)
   * - Other transactions: 1 item (single transaction)
   * 
   * @param conn Database connection
   * @param accountIds List of account IDs to create transactions for
   * @param categoryIds Map of category codes to IDs
   * @param count Number of transactions to create
   */
  def seedTransactionsWithItems(conn: Connection, accountIds: Seq[Int], 
                                categoryIds: Map[String, Int], count: Int): Unit = {

    val txnStmt = conn.prepareStatement(
      """INSERT INTO banking.transaction_header 
         (transaction_number, account_id, transaction_type, transaction_status, total_amount, 
          transaction_date, posting_date, channel, description)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
         RETURNING transaction_id"""
    )

    val itemStmt = conn.prepareStatement(
      """INSERT INTO banking.transaction_item 
         (transaction_id, item_sequence, category_id, item_amount, item_description, 
          payee_name, merchant_name, is_recurring)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?)"""
    )

    val txnTypes = Array("DEPOSIT", "WITHDRAWAL", "PAYMENT", "TRANSFER", "FEE")
    val channels = Array("ATM", "BRANCH", "ONLINE", "MOBILE", "PHONE")
    val statuses = Array("COMPLETED", "COMPLETED", "COMPLETED", "PENDING") // 75% completed

    // Sample merchants and payees for realistic transaction items
    val groceryStores = Array("Whole Foods", "Safeway", "Trader Joe's", "Kroger", "Walmart")
    val restaurants = Array("Chipotle", "Olive Garden", "Panera Bread", "Subway", "McDonald's")
    val utilities = Array("Pacific Gas & Electric", "Con Edison", "Comcast", "AT&T", "Verizon")

    (1 to count).foreach { i =>
      val accountId = accountIds(random.nextInt(accountIds.size))
      val txnType = txnTypes(random.nextInt(txnTypes.length))
      val status = statuses(random.nextInt(statuses.length))
      val channel = channels(random.nextInt(channels.length))
      
      // Transaction date within last 90 days (simulates 3 months of activity)
      val daysAgo = random.nextInt(90)
      val txnDate = Timestamp.valueOf(LocalDateTime.now().minusDays(daysAgo))

      // *** KEY LOGIC: Determine number of items based on transaction type ***
      val numItems = txnType match {
        case "PAYMENT"    => 1 + random.nextInt(5)  // 1-5 items (multiple bills paid together)
        case "WITHDRAWAL" => 1 + random.nextInt(3)  // 1-3 items (multiple purchases in one transaction)
        case _            => 1                       // Single item for deposits, transfers, fees
      }

      // Generate transaction items with realistic amounts and categories
      val items = (1 to numItems).map { itemSeq =>
        val (amount, category, description, payee, merchant, isRecurring) = txnType match {
          
          case "PAYMENT" =>
            // Simulate bill payment - utilities, credit card payments, etc.
            random.nextInt(3) match {
              case 0 => // Electricity bill
                val utility = utilities(random.nextInt(utilities.length))
                (50 + random.nextDouble() * 200, categoryIds("CAT-ELECTRIC"), 
                 s"Electric utility bill item $itemSeq", utility, null, random.nextBoolean())
              case 1 => // Internet/phone bill
                (40 + random.nextDouble() * 100, categoryIds("CAT-INTERNET"), 
                 s"Internet service payment item $itemSeq", utilities(random.nextInt(utilities.length)), null, true)
              case _ => // Water bill
                (30 + random.nextDouble() * 80, categoryIds("CAT-WATER"), 
                 s"Water bill payment item $itemSeq", "Water Department", null, true)
            }
          
          case "WITHDRAWAL" =>
            // Simulate shopping - groceries, restaurants, gas, etc.
            if (random.nextBoolean()) {
              val store = groceryStores(random.nextInt(groceryStores.length))
              (20 + random.nextDouble() * 150, categoryIds("CAT-GROCERY"), 
               s"Grocery purchase item $itemSeq", null, store, false)
            } else {
              val restaurant = restaurants(random.nextInt(restaurants.length))
              (15 + random.nextDouble() * 100, categoryIds("CAT-RESTAURANT"), 
               s"Restaurant purchase item $itemSeq", null, restaurant, false)
            }
          
          case "DEPOSIT" =>
            // Salary deposit or other income
            (1000 + random.nextDouble() * 4000, categoryIds("CAT-SALARY"), 
             "Payroll deposit", "Employer Inc", null, true)
          
          case "TRANSFER" =>
            // Internal transfer between accounts
            (100 + random.nextDouble() * 1000, categoryIds("CAT-INTERNAL"), 
             "Internal account transfer", null, null, false)
          
          case "FEE" =>
            // Bank service fee
            (5 + random.nextDouble() * 30, categoryIds.values.head, 
             "Bank service fee", "Bank", null, false)
        }

        (amount, category, description, payee, merchant, isRecurring, itemSeq)
      }

      // Calculate total transaction amount (sum of all items)
      val totalAmount = items.map(_._1).sum

      // Insert transaction header
      txnStmt.setString(1, f"TXN-2025-$i%06d") // Business key: TXN-2025-000001
      txnStmt.setInt(2, accountId)
      txnStmt.setString(3, txnType)
      txnStmt.setString(4, status)
      txnStmt.setBigDecimal(5, BigDecimal(totalAmount).setScale(2, BigDecimal.RoundingMode.HALF_UP).bigDecimal)
      txnStmt.setTimestamp(6, txnDate)
      txnStmt.setTimestamp(7, if (status == "COMPLETED") txnDate else null)
      txnStmt.setString(8, channel)
      txnStmt.setString(9, s"$txnType transaction with $numItems item(s)")

      val rs = txnStmt.executeQuery()
      rs.next()
      val transactionId = rs.getInt("transaction_id")

      // Insert all transaction items
      items.foreach { case (amount, categoryId, description, payee, merchant, isRecurring, itemSeq) =>
        itemStmt.setInt(1, transactionId)
        itemStmt.setInt(2, itemSeq) // Item sequence number (1, 2, 3, ...)
        itemStmt.setInt(3, categoryId)
        itemStmt.setBigDecimal(4, BigDecimal(amount).setScale(2, BigDecimal.RoundingMode.HALF_UP).bigDecimal)
        itemStmt.setString(5, description)
        if (payee != null) itemStmt.setString(6, payee) else itemStmt.setNull(6, java.sql.Types.VARCHAR)
        if (merchant != null) itemStmt.setString(7, merchant) else itemStmt.setNull(7, java.sql.Types.VARCHAR)
        itemStmt.setBoolean(8, isRecurring)
        itemStmt.executeUpdate()
      }

      // Commit in batches for performance and progress tracking
      if (i % 1000 == 0) {
        println(s"  ✓ Created $i transactions...")
        conn.commit()
      }
    }

    conn.commit()
    println(s"  ✓ Total transactions created: $count")
    
    // Calculate and display statistics
    val avgItemsQuery = conn.createStatement().executeQuery(
      """SELECT AVG(item_count)::numeric(10,2) as avg_items 
         FROM (SELECT COUNT(*) as item_count FROM banking.transaction_item GROUP BY transaction_id) t"""
    )
    avgItemsQuery.next()
    val avgItems = avgItemsQuery.getBigDecimal("avg_items")
    println(s"  ✓ Average items per transaction: $avgItems")
    
    val multiItemQuery = conn.createStatement().executeQuery(
      """SELECT COUNT(DISTINCT transaction_id) as multi_item_count 
         FROM (SELECT transaction_id, COUNT(*) as cnt FROM banking.transaction_item GROUP BY transaction_id HAVING COUNT(*) > 1) t"""
    )
    multiItemQuery.next()
    val multiItemCount = multiItemQuery.getInt("multi_item_count")
    println(s"  ✓ Transactions with multiple items: $multiItemCount (${(multiItemCount.toDouble / count * 100).toInt}%)")
  }
}

