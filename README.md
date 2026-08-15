# AURA (Automated User Relation Analyzer)

High-performance data transformation, parsing, and indexing engine built with Rust and ClickHouse for OSINT/GEOINT analytics.

## 🚀 Features

- **Fast CSV & Dump Parsing:** Memory-mapped streaming I/O for multi-gigabyte datasets.
- **Normalization Pipeline:** Automated phone/email/login canonicalization.
- **ClickHouse Integration:** High-throughput batch inserts using MergeTree engines.

## 🛠 Tech Stack

- **Language:** Rust, Python
- **Database:** ClickHouse
- **Key Crates:** `csv`, `serde`, `tokio`, `clickhouse-rs`

## 📦 Project Structure

- `db_formatter_rust/` — Core parsing and ETL CLI tool.
