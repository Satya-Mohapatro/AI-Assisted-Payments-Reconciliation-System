# AI-Assisted Payments Reconciliation System

[![Python](https://img.shields.io/badge/Python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![Streamlit](https://img.shields.io/badge/Streamlit-1.30+-red.svg)](https://streamlit.io/)
[![Pandas](https://img.shields.io/badge/Pandas-2.0+-green.svg)](https://pandas.pydata.org/)

A robust, automated financial reconciliation module designed to match business transactions with bank settlements. Built with an intuitive **Streamlit** dashboard and a highly resilient **Pandas** backbone, the system efficiently detects anomalies, flags edge cases effortlessly, and provides a clear breakdown of potential financial discrepancies.

##  Features

- **Automated Data Generation:** Built-in engine to generate synthetically complex datasets simulating real-world financial environments, inclusive of edge cases.
- **Robust Reconciliation Logic:** Matches business transactions with bank/PSP credits and debits based on deterministic identifiers and thresholds.
- **Comprehensive Issue Detection:** Automatically flags and categorizes inconsistencies, detailing exactly what went wrong.
- **Interactive Health Dashboard:** Simple, clean, and interactive user interface built with Streamlit for human-in-the-loop review.
- **Synthetic Unit Testing:** Comprehensive suite of unit tests validating accuracy across perfect matches and nuanced mismatches.

##  Handled Edge Cases

The discrepancy detection engine identifies various operational anomalies, such as:
- **`MISSING_SETTLEMENT`**: Transaction recorded, but no corresponding settlement arrived.
- **`ORPHAN_SETTLEMENT`**: Settlement landed, but no matching original transaction exists.
- **`AMOUNT_MISMATCH`**: Transaction and settlement values differ beyond acceptable thresholds.
- **`ROUNDING_MISMATCH`**: Slight differences in amounts (e.g., within a $0.02 tolerance).
- **`DELAYED_SETTLEMENT`**: Settlement arrived substantially later than expected.
- **`DUPLICATE_TRANSACTION/SETTLEMENT`**: Multiple identical records found for the same underlying ID.
- **`ORPHAN_REFUND`**: A refund was logged, but the original transaction wasn't found.

##  Tech Stack

- **Frontend UI Framework:** [Streamlit](https://streamlit.io/)
- **Data Processing Backend:** [Pandas](https://pandas.pydata.org/)
- **Testing:** Python `unittest`

##  Getting Started

### Prerequisites

You need Python 3 installed. It is recommended to use a virtual environment.

### 1. Installation

Clone this repository to your local machine:
```bash
git clone <your-repository-url>
cd AI-Assisted-Payments-Reconciliation-System
```

Install the required dependencies:
```bash
pip install -r requirements.txt
```

### 2. Running the Application

To launch the interactive dashboard, use Streamlit:
```bash
streamlit run app.py
```
This will open the application in your default web browser (typically at `http://localhost:8501`).

### 3. Running Background Tests

To execute the backend synthetic data logic and verify the core reconciliation tests, simply run:
```bash
python reconciliation_system.py
```

##  Project Structure

```text
├── app.py                     # Streamlit dashboard and UI logic
├── reconciliation_system.py   # Core logic for data generation, matching, and testing
├── reconciliation_summary.json# Sample generated summary output
├── reconciliation_detail.csv  # Sample detailed breakdown of flagged discrepancies
├── requirements.txt           # Python dependencies (Streamlit, Pandas, etc.)
└── README.md                  # Project documentation
```
