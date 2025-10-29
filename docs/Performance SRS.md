# **Software Requirements Specification (SRS)**

### **Project: CSV Duration Extractor CLI App**

---

## **1. Business Value**

The application aims to automate the extraction and normalization of duration data from performance monitoring CSV files.  
By standardizing mixed units (milliseconds and seconds) into a single consistent format (milliseconds), users can:

- Simplify performance analysis and reporting.

- Enable compatibility with analytical tools that require numeric-only datasets.

- Save time by automating manual conversions and CSV data cleanup.

- Ensure data integrity when aggregating performance metrics across sources.

---

## **2. Roles and Responsibilities**

| Role                                   | Responsibility                                                                                                      |
| -------------------------------------- | ------------------------------------------------------------------------------------------------------------------- |
| **Client / User (Analyst / Engineer)** | Provides input CSV file, executes the CLI, and verifies the output CSV.                                             |
| **Developer**                          | Implements the CLI tool, including parsing logic, conversion logic, and CSV handling.                               |
| **Tester / QA**                        | Validates that the CLI correctly processes both seconds and milliseconds inputs and outputs the correct CSV format. |
| **Maintainer / IT Support**            | Ensures the script runs on Windows 11 Pro and assists with dependency or environment setup.                         |

---

## **3. High-Level Process Description**

**Process Flow:**

1. **Start Application** → User runs the Python CLI via terminal (`python extract_duration.py input.csv output.csv`).

2. **Read Input File** → The app loads the UTF-8 encoded CSV.

3. **Extract "Duration" Column** → The app identifies and reads the “Duration” column.

4. **Normalize Values** →
   
   - If the value ends with “ms”, extract numeric part as is.
   
   - If the value ends with “s”, multiply numeric part by 1000.

5. **Generate Output File** → Write a single-column, numeric-only CSV file in UTF-8 encoding.

6. **End Process** → Display confirmation message to user.

---

## **4. User Stories and Acceptance Criteria**

### **User Story 1**

**As a** performance analyst,  
**I want** to extract the Duration column from a CSV file,  
**so that** I can analyze only the performance duration data.

**Acceptance Criteria:**

- The CLI accepts two arguments: input file path and output file path.

- If the input file is missing or invalid, the app should show an error.

- The output file is created or overwritten if it already exists.

---

### **User Story 2**

**As a** performance analyst,  
**I want** all durations expressed in milliseconds,  
**so that** I can easily compare values.

**Acceptance Criteria:**

- The tool recognizes both `ms` and `s` units.

- Values with `s` are multiplied by 1000.

- Output contains only numeric values, without headers.

- Example input: `2.1s → 2100` and `9.58ms → 9.58`.

---

### **User Story 3**

**As a** user,  
**I want** the app to run on Windows 11 Pro without extra dependencies,  
**so that** I can execute it easily in my environment.

**Acceptance Criteria:**

- The tool runs via Python 3 (no additional libraries beyond the standard library).

- The command-line interface accepts parameters correctly on Windows 11.

---

## **5. High-Level Solution Requirements**

### **Functional Requirements**

1. The app shall read an input CSV (UTF-8 encoded).

2. The app shall locate the “Duration” column dynamically (case-sensitive).

3. The app shall correctly interpret both milliseconds (`ms`) and seconds (`s`).

4. The app shall convert all values into milliseconds as floating-point numbers.

5. The app shall produce a single-column CSV with numeric-only values (no header).

6. The app shall handle missing or malformed values gracefully (skip or warn).

7. The app shall output logs or console messages indicating success or errors.

### **Non-Functional Requirements**

1. **Platform:** Windows 11 Pro.

2. **Language:** Python 3.8 or higher.

3. **Dependencies:** Only built-in modules (`csv`, `argparse`, `re`, etc.).

4. **Performance:** Must handle CSV files up to 100,000 rows in <5 seconds.

5. **Output Encoding:** UTF-8.

6. **Error Handling:** Informative error messages on invalid input or missing file.

---

## **6. CLI Command Example**

```bash
python extract_duration.py input.csv output.csv
```

**Expected Input Example:**

```
Date,Service,Resource,Duration,Method,Status Code
2025-10-27T15:23:23.859Z,/service,GET /resource,8.94ms,GET,200
2025-10-27T15:23:21.722Z,/service,GET /resource,2.04s,GET,200
2025-10-27T15:23:21.095Z,/service,GET /resource,1304.44ms,GET,200
2025-10-27T15:23:21.089Z,/service,GET /resource,140.51ms,GET,200
2025-10-27T15:23:20.680Z,/service,GET /resource,2.43s,GET,200
2025-10-27T15:23:20.675Z,/service,GET /resource,32.72ms,GET,200
2025-10-27T15:23:20.671Z,/service,GET /resource,1171.29ms,GET,200
```

see also input.csv

**Expected Output Example:**

```
8.94
2040
1304.44
140.51
2430
32.72
1171.29
```

see also output.csv
