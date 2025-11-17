"""
general_programming_logic_with_csv.py

- Contains solutions to assignment (functions for questions 1-22).
- Added CSV ingestion and analysis utilities for the large invoice dataset.
- Functions return values (suitable for unit tests); no input()/print() inside functions.
- Demo block at the end shows usage for loading the pasted CSV string or from a file.
"""

from typing import List, Dict, Any, Tuple, Optional
import csv
import io
from datetime import datetime
import math

# ---------- Assignment functions (Q1 - Q22) ----------
# (kept concise and non-printing)
def add_two_numbers(a: float, b: float) -> float:
    return a + b

def max_of_two(a: float, b: float) -> float:
    return a if a >= b else b

def is_even(number: int) -> bool:
    return number % 2 == 0

def grade_from_three_marks(mark1: float, mark2: float, mark3: float) -> str:
    average = (mark1 + mark2 + mark3) / 3.0
    if average > 90:
        return "A+"
    if 80 <= average <= 90:
        return "A"
    if 70 <= average < 80:
        return "A-"
    if 60 <= average < 70:
        return "B+"
    if 50 <= average < 60:
        return "B"
    return "FAIL"

def first_n_natural(n: int = 10) -> List[int]:
    return list(range(1, n + 1))

def first_n_natural_reverse(n: int = 10) -> List[int]:
    return list(range(n, 0, -1))

def first_n_even_natural(n: int = 10) -> List[int]:
    return [2 * i for i in range(1, n + 1)]

def numbers_between_range(start: int, end: int) -> List[int]:
    return list(range(start, end + 1)) if start <= end else list(range(start, end - 1, -1))

def multiplication_table(number: int, upto: int = 10) -> List[int]:
    return [number * i for i in range(1, upto + 1)]

def is_prime(number: int) -> bool:
    if number <= 1:
        return False
    if number <= 3:
        return True
    if number % 2 == 0:
        return False
    i = 3
    while i * i <= number:
        if number % i == 0:
            return False
        i += 2
    return True

def primes_in_range(start: int = 2, end: int = 100) -> List[int]:
    start = max(2, start)
    return [n for n in range(start, end + 1) if is_prime(n)]

def sum_of_digits(number: int) -> int:
    n = abs(number)
    total = 0
    while n:
        total += n % 10
        n //= 10
    return total

def lucky_number(number: int) -> int:
    n = abs(number)
    while n >= 10:
        n = sum_of_digits(n)
    return n

def reverse_number(number: int) -> int:
    sign = -1 if number < 0 else 1
    n = abs(number)
    rev = 0
    while n:
        rev = rev * 10 + n % 10
        n //= 10
    return sign * rev

def is_palindrome_number(number: int) -> bool:
    n = abs(number)
    return n == reverse_number(n)

def factorial(n: int) -> int:
    if n < 0:
        raise ValueError("Factorial is not defined for negative numbers.")
    result = 1
    for i in range(2, n + 1):
        result *= i
    return result

def nCr(n: int, r: int) -> int:
    if r < 0 or n < 0 or r > n:
        raise ValueError("Invalid n and r for nCr.")
    r = min(r, n - r)
    if r == 0:
        return 1
    num = 1
    den = 1
    for i in range(1, r + 1):
        num *= (n - r + i)
        den *= i
    return num // den

_DIGIT_WORDS = {0: "Zero",1:"One",2:"Two",3:"Three",4:"Four",5:"Five",6:"Six",7:"Seven",8:"Eight",9:"Nine"}
_TENS_WORDS = {
    10:"Ten",11:"Eleven",12:"Twelve",13:"Thirteen",14:"Fourteen",15:"Fifteen",
    16:"Sixteen",17:"Seventeen",18:"Eighteen",19:"Nineteen",20:"Twenty",
    30:"Thirty",40:"Forty",50:"Fifty",60:"Sixty",70:"Seventy",80:"Eighty",90:"Ninety"
}

def digit_to_word(digit: int) -> str:
    if digit not in _DIGIT_WORDS:
        raise ValueError("Input must be single digit 0..9")
    return _DIGIT_WORDS[digit]

def _two_digit_to_words(n: int) -> str:
    if n < 10:
        return _DIGIT_WORDS[n]
    if 10 <= n < 20:
        return _TENS_WORDS[n]
    tens = (n // 10) * 10
    ones = n % 10
    return _TENS_WORDS[tens] if ones == 0 else f"{_TENS_WORDS[tens]} {_DIGIT_WORDS[ones]}"

def number_to_words(number: int) -> str:
    if number == 0:
        return "Zero"
    if number < 0:
        return "Minus " + number_to_words(-number)

    def _hundreds_chunk_to_words(chunk: int) -> str:
        words = []
        if chunk >= 100:
            words.append(f"{_DIGIT_WORDS[chunk // 100]} Hundred")
            chunk %= 100
        if chunk > 0:
            words.append(_two_digit_to_words(chunk))
        return " ".join(words)

    parts = []
    scales = [
        (1_000_000_000_000, "Trillion"),
        (1_000_000_000, "Billion"),
        (1_000_000, "Million"),
        (1_000, "Thousand"),
        (1, "")
    ]
    n = number
    for scale_val, scale_name in scales:
        if n >= scale_val:
            chunk = n // scale_val
            n %= scale_val
            if scale_val == 1:
                parts.append(_hundreds_chunk_to_words(chunk))
            else:
                parts.append(f"{_hundreds_chunk_to_words(chunk)} {scale_name}")
    # cleanup empty strings
    return " ".join([p.strip() for p in parts if p.strip()])

# ---------- CSV handling & dataset utilities ----------
# The dataset columns: Invoice_ID,Customer_ID,Product_Name,Category,Quantity,Price,Total_Sales,Date
# Date format in dataset appears to be DD-MM-YYYY (example: 18-03-2024). We'll parse with dayfirst=True.

def _to_float_safe(value: str) -> Optional[float]:
    """Try to parse number-like string to float, return None if fails."""
    if value is None:
        return None
    v = value.strip()
    if v == "":
        return None
    try:
        return float(v)
    except Exception:
        # try removing commas
        try:
            return float(v.replace(",", ""))
        except Exception:
            return None

def parse_date_dayfirst(date_str: str) -> Optional[datetime]:
    """Parse date strings like '18-03-2024' (day-first). Returns datetime or None."""
    if not date_str:
        return None
    for fmt in ("%d-%m-%Y", "%d/%m/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(date_str.strip(), fmt)
        except Exception:
            continue
    # fallback try generic parse
    try:
        return datetime.fromisoformat(date_str.strip())
    except Exception:
        return None

def load_invoice_csv_from_string(csv_string: str) -> List[Dict[str, Any]]:
    """
    Load invoice CSV from a raw CSV string (like the one you pasted).
    Returns a list of dicts with typed fields.
    """
    f = io.StringIO(csv_string.strip())
    reader = csv.DictReader(f)
    rows = []
    for r in reader:
        # normalize keys (strip)
        row = {k.strip(): (v.strip() if isinstance(v, str) else v) for k, v in r.items()}
        # typed fields
        row['Quantity'] = int(float(row['Quantity'])) if row.get('Quantity', "") != "" else None
        row['Price'] = _to_float_safe(row.get('Price', ""))
        row['Total_Sales'] = _to_float_safe(row.get('Total_Sales', ""))
        row['Customer_ID'] = int(float(row['Customer_ID'])) if row.get('Customer_ID', "") != "" else None
        row['Date'] = parse_date_dayfirst(row.get('Date', ""))
        rows.append(row)
    return rows

def load_invoice_csv_from_file(path: str, encoding: str = "utf-8") -> List[Dict[str, Any]]:
    """
    Load invoice CSV from a file path.
    Returns list of typed dicts (same format as load_invoice_csv_from_string).
    """
    with open(path, "r", encoding=encoding) as f:
        data = f.read()
    return load_invoice_csv_from_string(data)

# ---------- Analysis functions (return data, no printing) ----------

def total_sales_by_category(rows: List[Dict[str, Any]]) -> Dict[str, float]:
    """
    Return dictionary {category: total_sales_sum}
    Ignores rows missing Total_Sales.
    """
    totals = {}
    for r in rows:
        cat = r.get('Category') or "Unknown"
        sales = r.get('Total_Sales')
        if sales is None:
            continue
        totals[cat] = totals.get(cat, 0.0) + sales
    return totals

def top_n_products_by_sales(rows: List[Dict[str, Any]], n: int = 10) -> List[Tuple[str, float]]:
    """
    Return list of (product_name, total_sales) sorted descending by sales.
    Aggregates across all rows for the same product name.
    """
    agg = {}
    for r in rows:
        prod = r.get('Product_Name') or "Unknown"
        sales = r.get('Total_Sales') or 0.0
        agg[prod] = agg.get(prod, 0.0) + sales
    sorted_items = sorted(agg.items(), key=lambda x: x[1], reverse=True)
    return sorted_items[:n]

def monthly_sales_summary(rows: List[Dict[str, Any]]) -> Dict[Tuple[int,int], float]:
    """
    Return dictionary {(year, month): total_sales_sum}
    month is 1..12. This is useful to plot monthly sales.
    """
    agg = {}
    for r in rows:
        dt = r.get('Date')
        sales = r.get('Total_Sales') or 0.0
        if not isinstance(dt, datetime):
            continue
        key = (dt.year, dt.month)
        agg[key] = agg.get(key, 0.0) + sales
    return dict(sorted(agg.items()))  # sorted by key (year, month)

def sales_by_customer(rows: List[Dict[str, Any]]) -> Dict[int, float]:
    """
    Return {customer_id: total_sales}. Rows with missing customer_id are skipped.
    """
    agg = {}
    for r in rows:
        cid = r.get('Customer_ID')
        sales = r.get('Total_Sales') or 0.0
        if cid is None:
            continue
        agg[cid] = agg.get(cid, 0.0) + sales
    return agg

def product_quantity_summary(rows: List[Dict[str, Any]]) -> Dict[str, int]:
    """
    Return {product_name: total_quantity_sold}
    """
    agg = {}
    for r in rows:
        prod = r.get('Product_Name') or "Unknown"
        qty = r.get('Quantity') or 0
        agg[prod] = agg.get(prod, 0) + qty
    return agg

def invoice_count_by_category(rows: List[Dict[str, Any]]) -> Dict[str, int]:
    """
    Number of invoices per category (simple count of rows).
    """
    counts = {}
    for r in rows:
        cat = r.get('Category') or "Unknown"
        counts[cat] = counts.get(cat, 0) + 1
    return counts

def highest_single_invoice(rows: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """
    Return the row (dict) which has the highest Total_Sales. None if empty or no sales.
    """
    best = None
    best_val = -math.inf
    for r in rows:
        s = r.get('Total_Sales')
        if s is None:
            continue
        if s > best_val:
            best_val = s
            best = r
    return best

def filter_rows_by_date_range(rows: List[Dict[str, Any]], start: Optional[datetime], end: Optional[datetime]) -> List[Dict[str, Any]]:
    """
    Filter rows occurring between start and end dates (inclusive).
    If start or end is None, it's treated as unbounded on that side.
    """
    filtered = []
    for r in rows:
        dt = r.get('Date')
        if not isinstance(dt, datetime):
            continue
        if start and dt < start:
            continue
        if end and dt > end:
            continue
        filtered.append(r)
    return filtered

# Export helpers (creates CSV text)
def export_aggregate_to_csv(aggregate: Dict[Any, Any], key_name: str = "Key", value_name: str = "Value") -> str:
    """
    Create a CSV text from a simple dict aggregate.
    Returns CSV content as string (you can write to file).
    """
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([key_name, value_name])
    for k, v in aggregate.items():
        writer.writerow([k, v])
    return output.getvalue()

# ---------- Demo (example usage) ----------
if __name__ == "__main__":
    # DEMO code: load CSV from a variable (paste your CSV into the string below) or from file path.
    # To use your pasted CSV, set paste_csv_string to the CSV text (the header + rows).
    paste_csv_string = """Invoice_ID,Customer_ID,Product_Name,Category,Quantity,Price,Total_Sales,Date
I-1,35,Tablet,Electronics,7,360.68,2524.76,18-03-2024
I-2,34,Rice,Groceries,6,128.33,769.98,26-02-2024
I-3,97,Yoga Mat,Sports,2,515.1,1030.2,08-12-2024
... (TRUNCATED FOR DEMO) ...
"""
    # NOTE: In practice paste the full CSV you sent (from Invoice_ID header on).
    # For demonstration we will not parse the truncated sample. To parse your full dataset,
    # replace paste_csv_string with the full CSV text you posted.

    # Example: if you have saved the CSV to disk:
    # rows = load_invoice_csv_from_file("invoices.csv")

    # Example: parse from string (uncomment and provide full CSV string)
    # rows = load_invoice_csv_from_string(paste_csv_string)

    # If running tests or unit tests, call functions directly:
    # Example stubs (remove when you run with real data)
    rows = []  # Replace with real rows loaded via the functions above.

    # Example usage (when rows is populated):
    # totals_by_cat = total_sales_by_category(rows)
    # top_products = top_n_products_by_sales(rows, n=10)
    # monthly = monthly_sales_summary(rows)
    # best_invoice = highest_single_invoice(rows)

    # The demo does not print by design. Use these functions in your script or unit tests,
    # or write small printing wrappers if you want immediate console output.

    pass
