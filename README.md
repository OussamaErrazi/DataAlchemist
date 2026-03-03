# 🧪 DataAlchemist

**DataAlchemist** is a lightweight, modular data transformation pipeline built with **Spring Boot**, **Kafka**, and **Docker**.  
The goal is simple: take raw data (JSON/CSV), clean it, transform it, and export it — all through a Kafka-powered pipeline.

## 🎯 Transformation Pipeline Implementation Current Goals

✅ -> implemented | ❌ -> not yet implemented

### ✅ Goal 1: Column Reference by Position

Columns can be referenced like this `%i` with i the position of the column starting from 1.

**Example:** `%2` in this table `["id", "name", "salary"]` is referencing the name column

---

### ✅ Goal 2: Column Selection

Select specific columns from the dataset.

**Example:** `%1, %3` in table `["id", "name", "salary"]` would select only id and salary columns

---

### ✅ Goal 3: Column Aliasing

Rename columns during selection using `as` keyword.

**Example:** `%1 as employee_id, %2 as full_name` would rename 1st column to employee_id and 2nd column to full_name

---

### ✅ Goal 4: Basic Arithmetic Operations

Perform calculations between columns using `+`, `-`, `*`, `/` operators.

**Example:** `(%3 + %4) as total` with table `["id", "name", "base", "bonus"]` would add base and bonus columns

---

### ❌ Goal 5: Numeric Comparison Filters

Filter rows using comparison operators `>`, `<`, `>=`, `<=`, `==`, `!=`.

**Example:** `%3 >= 5000` would keep only rows where the third column value is 5000 or higher

---

### ❌ Goal 6: String Equality Filters

Filter rows based on exact string matches.

**Example:** `%2 == "IT"` would keep only rows where the second column equals "IT"

---

### ❌ Goal 7: Regex Pattern Matching

Filter rows using regex patterns with `~` operator.

**Example:** `%3 ~ ".*@company\\.com"` would keep only rows where the third column matches the email pattern

---

### ❌ Goal 8: Logical Operators

Combine multiple filter conditions using `AND`, `OR` operators.

**Example:** `%3 >= 5000 AND %4 == "IT"` would keep rows where salary is 5000+ AND department is IT

---

### ❌ Goal 9: Column Name Assertions

Assert that a column position corresponds to a specific column name using `is` operator.

**Example:** `%1 is id, %2 is employee_name` would verify that the first column is named "id" and second is "employee_name"

---

### ❌ Goal 10: Composite Expressions

Combine multiple expressions in one.

**Example:** `"%1 + %2 + %3 as total"` compute addition of more than two columns and name it total
