# 🧪 DataAlchemist

**DataAlchemist** is a lightweight, modular data transformation pipeline built with **Spring Boot**, **Kafka**, and **Docker**.  
The goal is simple: take raw data (JSON/CSV), clean it, transform it, and export it — all through a Kafka-powered pipeline.

## 🎯 Currently Implemented Goals

✅ -> implemented | ❌ -> not yet implemented

### ✅ Goal 1: Column Reference by Position

Columns can be referenced like this `%i` with i the position of the column starting from 1.

**Example:** `%2` in this table `["id", "name", "salary"]` is referencing the name column

---

### ✅ Goal 2: Column Selection

Select specific columns from the dataset.

**Example:** `%1, %3` in table `["id", "name", "salary"]` would select only id and salary columns

---

### ✅ Goal 3: Literal Values in Expressions

Use raw values as operands alongside column references in any expression.

**Example:** `%2 + " Jr." as "full_name"` appends a string literal to the second column.

---

### ✅ Goal 4: Column Aliasing

Rename columns during selection using `as` keyword.

**Example:** `%1 as "employee_id", %2 as "full_name"` would rename 1st column to employee_id and 2nd column to full_name

---

### ✅ Goal 5: Basic Arithmetic Operations

Perform calculations between columns using `+`, `-`, `*`, `/` operators.

**Example:** `(%5 + %3 * %4) as total` with table `["id", "name", "hours attended", "base", "bonus"]` would add bonus to (base \* hours attended) and name column "total"

---

### ✅ Goal 6: Comparison Operations

Perform comparison operations using the operators : `>`, `<`, `>=`, `<=`, `==`, `!=`.

**Example:** `%3 >= 5000` returns a boolean column — true when the third column is 5000 or more, otherwise false

---

### ✅ Goal 7: Regex Pattern Matching

Perform regex patterns evaluation with `~` operator.

**Example:** `%3 ~ ".*@company\\.com"` would keep only rows where the third column matches the email pattern

---

### ✅ Goal 8: Logical Operators

Combine multiple conditions using `AND`, `OR` operators.

**Example:** `%3 >= 5000 AND %4 == "IT"` evaluate expression salary is 5000+ AND department is IT

---

### ✅ Goal 9: Column Type Casting

Cast type to a column using `is` operator.

**Example:** `%1, %2 is integer` convert second column type to integer (ex: double to integer)

---

### ✅ Goal 10: Composite Expressions

Combine multiple expressions in one.

**Example:** `"%1 * 2 + %2 + %3 as "new column" is integer"` compute addition|multiplication of more than two columns and name it total + convert type to integer

---

### ✅ Goal 11: Conditional Expression

Produce a value based on a condition using the IF function

**Example:** `if(%3 > 5000, "high", "low") as "salary_range"` return "high" when salary exceeds 5000, otherwise return "low"

---

### ❌ Goal 12: String Functions

Apply built-in string transformation functions to column values

**Example:** `UPPER(%2) as "name"` converts the second column to uppercase. while `LOWER(%2) as "name"` converts to lowercase.

- other functions :
  - `trim(string expression)`
  - `contains(string expression, keyword)`
  - `length(string expression)`
  - `substring(string expression, from, to)`

---

### ✅ Goal 13: Handle Null

Check whether a column value is null or not null using == and != operators and the null keyword.

**Example:** `%3 != null` returns true when the third column is not null. Can be used inside functions and other expressions to handle null values.

---

### ✅ Goal 14: Null FallBack

Provide default value for missing data

**Example:** `default(%3, 0)` reference 3rd column, default to 0 if value is null
