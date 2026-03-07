# 🧪 DataAlchemist

**DataAlchemist** is a lightweight, modular data transformation pipeline built with **Spring Boot**, **Kafka**, and **Docker**.  
The goal is simple: take raw data (JSON/CSV), clean it, transform it, and export it — all through a Kafka-powered pipeline.

# Definition

## Stage Expression

A pipeline is defined as an ordered list of stage expressions. Each stage expression takes the output row of the previous stage as its input and produces a new row.
Column references like `%1`, `%2` always refer to the columns of the current stage input, not the original dataset.

A stage expression is a comma-separated list of column expressions where each one defines one output column. The output row of a stage contains exactly the columns defined by its expressions in the same order.

**Example:** given a dataset with columns `["id", "first_name", "last_name", "salary"]`:

```
pipeline = [
             "%1, %2 + \" \" + %3 as \"full_name\", %4" ,
             "%1, %2, %3 * 1.1 as \"new_salary\""
           ]

Stage 1: "%1, %2 + " " + %3 as "full_name", %4"
    input:  [id, first_name, last_name, salary]
    output: [id, full_name, salary]

Stage 2: "%1, %2, %3 * 1.1 as "new_salary""
    input:  [id, full_name, salary]      <- output of stage 1
    output: [id, full_name, new_salary]  <- %3 here is salary not last_name
```

There are currently two types of stages:

- **Transformation stage** : reshapes, computes, and selects columns
- **Filter stage** : drops rows that do not match a condition using `filter(...)`

### ❌ Condition ❌ :

A stage must be of a single type. Mixing operations from different stage types in the same stage is invalid.

### ℹ️ Note ℹ️ :

in what follows this emoji ✅ marks a goal as implemented while this emoji ❌ means not yet implemented

## 🎯 Transformation Stage Expressions

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

Combine multiple conditions using `AND`, `OR` operators, in addition to `NOT` as a function.

**Example:** `%3 >= 15000 AND NOT(%4 == "IT")` evaluate expression salary is 15000+ AND department is not IT

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

### ✅ Goal 12: String Functions

Apply built-in string transformation functions to column values

**Example:** `UPPER(%2) as "name"` converts the second column to uppercase. while `LOWER(%2) as "name"` converts to lowercase.

- other functions :
  - `trim(string expression)`
  - `contains(string expression, string keyword)`
  - `length(string expression)`
  - `substring(string expression, from index::included, to index::not included)`
  - `index_of(string expression, string keyword)`
  - `starts_with(string expression, string keyword)`
  - `ends_with(string expression, string keyword)`
  - `replace(string expression, string target, string replacement)`

---

### ✅ Goal 13: Handle Null

Check whether a column value is null or not null using == and != operators and the null keyword.

**Example:** `%3 != null` returns true when the third column is not null. Can be used inside functions and other expressions to handle null values.

---

### ✅ Goal 14: Null FallBack

Provide default value for missing data

**Example:** `default(%3, 0)` reference 3rd column, default to 0 if value is null

---

### ✅ Goal 15: Date Function

Represent a date var inside an expression using the `date(dd, MM, yyyy)` function.
Any field can be left empty to inherit that field from the context date in the operation.
The reader service automatically detects date columns using the formats `yyyy-MM-dd`, `MM/dd/yyyy`, `dd-MM-yyyy` and converts them to `dd/MM/yyyy` before reaching the transformation service.

**Example:** `date(%2, %3, 2026)` builds a date using the day from column 2, the month from column 3, and a literal year 2026. `%6 == date(, , 2024)` checks if the date in column 6 falls in the year 2024

---

### ✅ Goal 16: Date Functions

Extract components from a date column or perform computations between two dates.

**Example:** `get_day(%6) as "admission_day"` extracts the day number from the admission date column. `days_between(%6, today()) as "days_since"` computes the number of days between the admission date and today.

- extraction functions:
  - `get_day(date expression)` extracts the day as an integer
  - `get_month(date expression)` extracts the month as an integer
  - `get_year(date expression)` extracts the year as an integer

- computation functions:
  - `days_between(date expression, date expression)` number of days between two dates
  - `months_between(date expression, date expression)` number of months between two dates
  - `years_between(date expression, date expression)` number of years between two dates
  - `today()` current date

---

# 🎯 Filter Stage Expressions

a filter stage can consiste only of one filter function.

### ✅ Goal 1: Filter function

filter function used to filter rows by a condition

**Example:** `filter(%5 >= 15000 and %6 == "IT")` keeps rows where salary is 15000+ AND department is IT
