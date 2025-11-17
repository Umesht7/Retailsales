"""
sales_analysis.py

Improved, production-ready script to load, clean, analyze and visualize the provided invoice CSV.
Drop this file into a GitHub repo and push — it includes CLI, logging, and export of results.

Usage example:
    python sales_analysis.py --input data/invoices.csv --outdir outputs

Requirements (pip):
    pandas
    matplotlib

This file intentionally avoids heavy plotting libraries so it's easy to run anywhere.
"""

from __future__ import annotations
import argparse
import logging
from pathlib import Path
from typing import Tuple

import pandas as pd
import matplotlib.pyplot as plt

# ---------------------------
# Configuration
# ---------------------------
LOG_FMT = "%(asctime)s %(levelname)s: %(message)s"
DATE_COL = "Date"

# ---------------------------
# Helpers
# ---------------------------

def configure_logging(level: str = "INFO") -> None:
    logging.basicConfig(level=getattr(logging, level.upper()), format=LOG_FMT)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Analyze invoice CSV and produce summaries + plots")
    p.add_argument("--input", "-i", required=True, help="Path to the invoice CSV file")
    p.add_argument("--outdir", "-o", default="outputs", help="Directory to save outputs")
    p.add_argument("--verbose", "-v", action="store_true", help="Enable debug logging")
    return p.parse_args()


# ---------------------------
# Data functions
# ---------------------------

def load_data(path: Path) -> pd.DataFrame:
    """Load CSV to DataFrame and coerce types."""
    logging.info("Loading data from %s", path)
    df = pd.read_csv(path)

    # Standardize column names (strip spaces)
    df.columns = [c.strip() for c in df.columns]

    # Parse date column - handle day-month-year formats like 18-03-2024
    if DATE_COL in df.columns:
        df[DATE_COL] = pd.to_datetime(df[DATE_COL], dayfirst=True, errors="coerce")
    else:
        logging.warning("Date column '%s' not found", DATE_COL)

    # Numeric coercion for Price, Quantity, Total_Sales
    for col in ["Quantity", "Price", "Total_Sales"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """Perform cleaning: drop rows with no invoice id or all-NaN, fill reasonable defaults."""
    logging.info("Cleaning data (dropping empty rows and rows without Invoice_ID)")
    df = df.copy()
    # Drop fully empty rows
    df.dropna(how="all", inplace=True)

    # Ensure Invoice_ID exists
    if "Invoice_ID" in df.columns:
        df = df[df["Invoice_ID"].notna()]

    # Fill small issues
    if "Quantity" in df.columns:
        df["Quantity"].fillna(0, inplace=True)
        df["Quantity"] = df["Quantity"].astype(int)

    # If Total_Sales missing but Quantity & Price present, compute it
    if all(c in df.columns for c in ("Quantity", "Price")) and "Total_Sales" in df.columns:
        missing_total_mask = df["Total_Sales"].isna()
        if missing_total_mask.any():
            logging.info("Computing missing Total_Sales for %d rows", missing_total_mask.sum())
            df.loc[missing_total_mask, "Total_Sales"] = (
                df.loc[missing_total_mask, "Quantity"] * df.loc[missing_total_mask, "Price"]
            )

    return df


# ---------------------------
# Analysis functions
# ---------------------------

def sales_by_category(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate total sales and quantity by Category."""
    logging.info("Aggregating sales by Category")
    if "Category" not in df.columns:
        raise KeyError("Column 'Category' not found in data")

    agg = (
        df.groupby("Category")[ ["Quantity", "Total_Sales"] ]
        .sum(numeric_only=True)
        .sort_values("Total_Sales", ascending=False)
        .reset_index()
    )
    return agg


def monthly_sales(df: pd.DataFrame) -> pd.DataFrame:
    """Return monthly total sales timeseries (year-month)."""
    logging.info("Computing monthly sales time series")
    if DATE_COL not in df.columns:
        raise KeyError(f"Column '{DATE_COL}' not found")

    tmp = df[[DATE_COL, "Total_Sales"]].dropna()
    tmp = tmp.set_index(DATE_COL)
    monthly = tmp["Total_Sales"].resample("M").sum().rename("Total_Sales").reset_index()
    monthly["YearMonth"] = monthly[DATE_COL].dt.to_period("M").astype(str)
    return monthly[["YearMonth", "Total_Sales"]]


def top_n_products(df: pd.DataFrame, n: int = 10) -> pd.DataFrame:
    logging.info("Getting top %d products by Total_Sales", n)
    return (
        df.groupby("Product_Name")["Total_Sales"]
        .sum(numeric_only=True)
        .sort_values(ascending=False)
        .head(n)
        .reset_index()
    )


# ---------------------------
# Output helpers
# ---------------------------

def save_csv(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)
    logging.info("Saved CSV: %s", path)


def plot_bar(df: pd.DataFrame, x: str, y: str, title: str, path: Path) -> None:
    """Simple bar plot saved to path."""
    logging.info("Creating bar plot: %s -> %s", x, y)
    fig, ax = plt.subplots(figsize=(8, 5))
    df.plot(kind="bar", x=x, y=y, ax=ax, legend=False)
    ax.set_title(title)
    ax.set_ylabel(y)
    plt.tight_layout()
    plt.savefig(path)
    plt.close(fig)
    logging.info("Saved plot: %s", path)


def plot_timeseries(df: pd.DataFrame, x: str, y: str, title: str, path: Path) -> None:
    logging.info("Creating timeseries plot: %s -> %s", x, y)
    fig, ax = plt.subplots(figsize=(10, 4))
    ax.plot(df[x], df[y], marker="o")
    ax.set_title(title)
    ax.set_xlabel(x)
    ax.set_ylabel(y)
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(path)
    plt.close(fig)
    logging.info("Saved plot: %s", path)


# ---------------------------
# Main pipeline
# ---------------------------

def run(input_path: Path, outdir: Path) -> None:
    outdir.mkdir(parents=True, exist_ok=True)

    df = load_data(input_path)
    df = clean_data(df)

    # Save cleaned data
    save_csv(df, outdir / "cleaned_invoices.csv")

    # Analysis 1: sales by category
    cat = sales_by_category(df)
    save_csv(cat, outdir / "sales_by_category.csv")
    plot_bar(cat, x="Category", y="Total_Sales", title="Total Sales by Category", path=outdir / "sales_by_category.png")

    # Analysis 2: monthly sales
    monthly = monthly_sales(df)
    save_csv(monthly, outdir / "monthly_sales.csv")
    plot_timeseries(monthly, x="YearMonth", y="Total_Sales", title="Monthly Total Sales", path=outdir / "monthly_sales.png")

    # Analysis 3: top products
    top_products = top_n_products(df, n=15)
    save_csv(top_products, outdir / "top_products.csv")
    plot_bar(top_products, x="Product_Name", y="Total_Sales", title="Top Products by Sales (Top 15)", path=outdir / "top_products.png")

    logging.info("All outputs saved to %s", outdir)


# ---------------------------
# Entry point
# ---------------------------

if __name__ == "__main__":
    args = parse_args()
    configure_logging("DEBUG" if args.verbose else "INFO")

    input_path = Path(args.input)
    outdir = Path(args.outdir)

    try:
        run(input_path, outdir)
    except Exception as e:
        logging.exception("Processing failed: %s", e)
        raise


