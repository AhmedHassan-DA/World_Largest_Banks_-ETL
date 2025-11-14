# Bank Market Cap ETL Pipeline

**Automated ETL pipeline to extract the top 10 largest banks by market capitalization, convert market-cap values into GBP / EUR / INR using external exchange-rate data, and persist results to CSV and SQLite for downstream analysis.**

---

## Fictional use case
Built for the *Global Financial Research Institute (GFRI)* to produce quarterly investor briefing packs. The pipeline scrapes a historical snapshot of a public source, normalizes market-cap values, converts currencies using a provided exchange-rate CSV, stores results for analysis, and logs progress for auditability.

---

## Features
- Web scraping (Wayback Machine snapshot of Wikipedia) to extract the "By market capitalization" table
- Data cleaning and normalization (top 10 banks)
- Multi-currency conversion (USD → GBP, EUR, INR) using an exchange-rate CSV
- Output persistence to CSV and SQLite database
- Structured progress logging
- Simple SQL query examples for analysis

---

## File structure (recommended)
