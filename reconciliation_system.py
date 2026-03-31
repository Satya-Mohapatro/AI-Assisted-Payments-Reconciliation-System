"""
=============================================================
  Financial Reconciliation System  —  MVP
=============================================================

ASSUMPTIONS
-----------
1.  A "transaction" is any payment initiated by the business
    (sale, refund, fee, etc.).
2.  A "settlement" is the bank/PSP credit/debit that should
    correspond 1-to-1 with a transaction.
3.  Matching key  : transaction_id  (present in both datasets).
4.  Tolerance     : amounts are considered matching if they
    differ by ≤ $0.02 (rounding tolerance).
5.  Delayed settlement: settlement date > transaction date + 3 days.
6.  Duplicate     : same transaction_id appears > 1 time in
    either dataset.
7.  Orphan        : settlement exists with no matching
    transaction_id in the transactions dataset.
8.  Missing       : transaction exists with no matching
    settlement_id in the settlements dataset.
9.  Refund-without-original: a transaction of type "refund"
    whose original_txn_id does not exist in the transactions
    dataset.
10. All amounts are in USD; negative amounts represent refunds.
"""

import json
import random
import unittest
from datetime import date, timedelta
from io import StringIO

import pandas as pd

# ─────────────────────────────────────────────────────────────
#  Constants
# ─────────────────────────────────────────────────────────────
ROUNDING_TOLERANCE = 0.02   # dollars
DELAY_THRESHOLD    = 3      # calendar days


# =============================================================
#  1.  DATA GENERATION
# =============================================================

def generate_data(seed: int = 42) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Return (transactions_df, settlements_df) with embedded edge cases:

    Edge cases injected
    -------------------
    * TXN-005  — delayed settlement (settled 40 days later, cross-month)
    * TXN-006  — rounding mismatch  (settlement off by $0.01)
    * TXN-007  — duplicate transaction entry
    * TXN-008  — duplicate settlement entry
    * TXN-009  — transaction with NO matching settlement (missing settlement)
    * TXN-010  — orphan settlement (settlement with no transaction)
    * REF-001  — refund whose original_txn_id (TXN-999) does not exist
    """
    random.seed(seed)
    base_date = date(2024, 3, 1)

    # ── Core transactions ──────────────────────────────────────
    transactions = [
        # Normal matches
        {"txn_id": "TXN-001", "amount":  100.00, "date": base_date,
         "type": "sale",   "original_txn_id": None},
        {"txn_id": "TXN-002", "amount":  250.50, "date": base_date + timedelta(1),
         "type": "sale",   "original_txn_id": None},
        {"txn_id": "TXN-003", "amount":   75.00, "date": base_date + timedelta(2),
         "type": "sale",   "original_txn_id": None},
        {"txn_id": "TXN-004", "amount":  -30.00, "date": base_date + timedelta(3),
         "type": "refund", "original_txn_id": "TXN-001"},   # valid refund

        # Edge cases
        {"txn_id": "TXN-005", "amount":  500.00, "date": base_date + timedelta(4),
         "type": "sale",   "original_txn_id": None},         # delayed settlement

        {"txn_id": "TXN-006", "amount":  199.99, "date": base_date + timedelta(5),
         "type": "sale",   "original_txn_id": None},         # rounding mismatch

        {"txn_id": "TXN-007", "amount":   88.00, "date": base_date + timedelta(6),
         "type": "sale",   "original_txn_id": None},         # duplicate txn

        {"txn_id": "TXN-008", "amount":   55.00, "date": base_date + timedelta(7),
         "type": "sale",   "original_txn_id": None},         # duplicate settlement

        {"txn_id": "TXN-009", "amount":  320.00, "date": base_date + timedelta(8),
         "type": "sale",   "original_txn_id": None},         # NO settlement

        # Refund without original transaction
        {"txn_id": "REF-001", "amount":  -50.00, "date": base_date + timedelta(9),
         "type": "refund", "original_txn_id": "TXN-999"},    # TXN-999 doesn't exist
    ]

    # Inject duplicate transaction row
    transactions.append(
        {"txn_id": "TXN-007", "amount": 88.00, "date": base_date + timedelta(6),
         "type": "sale", "original_txn_id": None}
    )

    txn_df = pd.DataFrame(transactions)
    txn_df["date"] = pd.to_datetime(txn_df["date"])

    # ── Settlements ────────────────────────────────────────────
    settlements = [
        # Normal matches
        {"settlement_id": "SET-001", "txn_id": "TXN-001",
         "amount":  100.00, "settlement_date": base_date + timedelta(1)},
        {"settlement_id": "SET-002", "txn_id": "TXN-002",
         "amount":  250.50, "settlement_date": base_date + timedelta(2)},
        {"settlement_id": "SET-003", "txn_id": "TXN-003",
         "amount":   75.00, "settlement_date": base_date + timedelta(3)},
        {"settlement_id": "SET-004", "txn_id": "TXN-004",
         "amount":  -30.00, "settlement_date": base_date + timedelta(4)},

        # Delayed settlement (cross-month, 40 days later)
        {"settlement_id": "SET-005", "txn_id": "TXN-005",
         "amount":  500.00, "settlement_date": base_date + timedelta(44)},

        # Rounding mismatch ($199.99 → settled as $200.00)
        {"settlement_id": "SET-006", "txn_id": "TXN-006",
         "amount":  200.00, "settlement_date": base_date + timedelta(6)},

        # Settlement for duplicate transaction
        {"settlement_id": "SET-007", "txn_id": "TXN-007",
         "amount":   88.00, "settlement_date": base_date + timedelta(7)},

        # Duplicate settlement rows
        {"settlement_id": "SET-008", "txn_id": "TXN-008",
         "amount":   55.00, "settlement_date": base_date + timedelta(8)},

        # REF-001 is settled normally
        {"settlement_id": "SET-009", "txn_id": "REF-001",
         "amount":  -50.00, "settlement_date": base_date + timedelta(10)},

        # Orphan settlement — no matching transaction
        {"settlement_id": "SET-010", "txn_id": "TXN-GHOST",
         "amount":  999.00, "settlement_date": base_date + timedelta(5)},
    ]

    # Inject duplicate settlement row
    settlements.append(
        {"settlement_id": "SET-008", "txn_id": "TXN-008",
         "amount": 55.00, "settlement_date": base_date + timedelta(8)}
    )

    set_df = pd.DataFrame(settlements)
    set_df["settlement_date"] = pd.to_datetime(set_df["settlement_date"])

    return txn_df, set_df


# =============================================================
#  2.  RECONCILIATION LOGIC
# =============================================================

def reconcile_data(
    txn_df: pd.DataFrame,
    set_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Merge transactions and settlements on txn_id.
    Returns a merged DataFrame used by detect_issues().
    Uses outer join to catch both missing and orphan records.
    De-duplicates first so merging is deterministic.
    """
    # Mark duplicates before dropping — we'll report them later
    txn_deduped = txn_df.copy()
    txn_deduped["txn_is_duplicate"] = txn_deduped.duplicated(
        subset=["txn_id"], keep=False
    )
    txn_deduped = txn_deduped.drop_duplicates(subset=["txn_id"], keep="first")

    set_deduped = set_df.copy()
    # Bug fix: duplicates in settlements are identified by settlement_id, not txn_id.
    # Using txn_id would wrongly flag two different settlements for the same transaction.
    set_deduped["set_is_duplicate"] = set_deduped.duplicated(
        subset=["settlement_id"], keep=False
    )
    set_deduped = set_deduped.drop_duplicates(subset=["settlement_id"], keep="first")

    merged = txn_deduped.merge(
        set_deduped,
        on="txn_id",
        how="outer",
        suffixes=("_txn", "_set"),
    )

    # Fill NaN flags
    merged["txn_is_duplicate"] = merged["txn_is_duplicate"].fillna(False)
    merged["set_is_duplicate"] = merged["set_is_duplicate"].fillna(False)

    return merged


# =============================================================
#  3.  ISSUE DETECTION
# =============================================================

def detect_issues(
    merged: pd.DataFrame,
    txn_df: pd.DataFrame,
    tolerance: float = ROUNDING_TOLERANCE,
    delay_days: int = DELAY_THRESHOLD,
) -> tuple[dict, pd.DataFrame]:
    """
    Inspect the merged DataFrame and return:
      - summary  : dict  (JSON-serialisable summary report)
      - detail_df: DataFrame of flagged rows with issue labels
    """
    issues = []

    for _, row in merged.iterrows():
        row_issues = []

        has_txn = pd.notna(row.get("txn_id")) and pd.notna(row.get("amount_txn"))
        has_set = pd.notna(row.get("settlement_id")) and pd.notna(row.get("amount_set"))

        # ── Duplicate transaction ──────────────────────────────
        if row.get("txn_is_duplicate"):
            row_issues.append("DUPLICATE_TRANSACTION")

        # ── Duplicate settlement ───────────────────────────────
        if row.get("set_is_duplicate"):
            row_issues.append("DUPLICATE_SETTLEMENT")

        # ── Missing settlement (transaction has no settlement) ─
        if has_txn and not has_set:
            row_issues.append("MISSING_SETTLEMENT")

        # ── Orphan settlement (settlement has no transaction) ──
        if has_set and not has_txn:
            row_issues.append("ORPHAN_SETTLEMENT")

        # ── Amount mismatch ────────────────────────────────────
        if has_txn and has_set:
            diff = abs(row["amount_txn"] - row["amount_set"])
            if diff > tolerance:
                row_issues.append("AMOUNT_MISMATCH")
            elif 0 < diff <= tolerance:
                row_issues.append("ROUNDING_MISMATCH")

        # ── Delayed settlement ─────────────────────────────────
        if has_txn and has_set:
            days_late = (row["settlement_date"] - row["date"]).days
            if days_late > delay_days:
                row_issues.append(f"DELAYED_SETTLEMENT({days_late}d)")

        # ── Refund without original transaction ───────────────
        if has_txn and row.get("type") == "refund":
            orig_id = row.get("original_txn_id")
            if pd.notna(orig_id) and orig_id not in txn_df["txn_id"].values:
                row_issues.append("ORPHAN_REFUND")

        if row_issues:
            issues.append({
                "txn_id":          row.get("txn_id"),
                "settlement_id":   row.get("settlement_id"),
                "amount_txn":      row.get("amount_txn"),
                "amount_set":      row.get("amount_set"),
                "txn_date":        str(row["date"])[:10] if pd.notna(row.get("date")) else None,
                "settlement_date": str(row["settlement_date"])[:10]
                                   if pd.notna(row.get("settlement_date")) else None,
                "issues":          row_issues,
            })

    detail_df = pd.DataFrame(issues) if issues else pd.DataFrame()

    # ── Build summary ──────────────────────────────────────────
    def _count(tag: str) -> int:
        return sum(1 for r in issues if any(tag in i for i in r["issues"]))

    total_txn   = int(merged["txn_id"].notna().sum())       # after dedup
    total_set   = int(merged["settlement_id"].notna().sum())

    # Bug fix: clean_matches must be rows that have BOTH sides and appear in NO issues list.
    # Old formula (total_txn - len(issues)) was wrong: issues includes orphan settlement rows
    # that have no txn side, so the subtraction could undercount or go negative.
    flagged_txn_ids = {r["txn_id"] for r in issues if r["txn_id"] is not None}
    both_sides = merged[merged["txn_id"].notna() & merged["settlement_id"].notna()]
    clean_matches = int((~both_sides["txn_id"].isin(flagged_txn_ids)).sum())

    summary = {
        "total_transactions":       total_txn,
        "total_settlements":        total_set,
        "clean_matches":            clean_matches,
        "issues_found":             len(issues),
        "breakdown": {
            "missing_settlement":   _count("MISSING_SETTLEMENT"),
            "orphan_settlement":    _count("ORPHAN_SETTLEMENT"),
            "amount_mismatch":      _count("AMOUNT_MISMATCH"),
            "rounding_mismatch":    _count("ROUNDING_MISMATCH"),
            "delayed_settlement":   _count("DELAYED_SETTLEMENT"),
            "duplicate_transaction":_count("DUPLICATE_TRANSACTION"),
            "duplicate_settlement": _count("DUPLICATE_SETTLEMENT"),
            "orphan_refund":        _count("ORPHAN_REFUND"),
        },
    }

    return summary, detail_df


# =============================================================
#  4.  PIPELINE RUNNER
# =============================================================

def run_reconciliation(
    txn_df: pd.DataFrame | None = None,
    set_df: pd.DataFrame | None = None,
    seed: int = 42,
    verbose: bool = True,
) -> tuple[dict, pd.DataFrame]:
    """End-to-end pipeline. Generates data if not provided."""
    if txn_df is None or set_df is None:
        txn_df, set_df = generate_data(seed=seed)

    merged    = reconcile_data(txn_df, set_df)
    summary, detail_df = detect_issues(merged, txn_df)

    if verbose:
        print("=" * 60)
        print("  RECONCILIATION SUMMARY")
        print("=" * 60)
        print(json.dumps(summary, indent=2))
        if not detail_df.empty:
            print("\n  DETAILED MISMATCHES")
            print("-" * 60)
            pd.set_option("display.max_colwidth", 40)
            print(detail_df.to_string(index=False))
        print("=" * 60)

    return summary, detail_df


# =============================================================
#  5.  UNIT TESTS
# =============================================================

class TestReconciliation(unittest.TestCase):

    # ── helpers ───────────────────────────────────────────────

    def _make_txn(self, rows):
        if not rows:
            return pd.DataFrame(columns=[
                "txn_id", "amount", "date", "type", "original_txn_id"
            ])
        df = pd.DataFrame(rows)
        df["date"] = pd.to_datetime(df["date"])
        if "type" not in df.columns:
            df["type"] = "sale"
        if "original_txn_id" not in df.columns:
            df["original_txn_id"] = None
        return df

    def _make_set(self, rows):
        if not rows:
            return pd.DataFrame(columns=[
                "settlement_id", "txn_id", "amount", "settlement_date"
            ])
        df = pd.DataFrame(rows)
        df["settlement_date"] = pd.to_datetime(df["settlement_date"])
        return df

    def _run(self, txn_rows, set_rows):
        txn_df = self._make_txn(txn_rows)
        set_df = self._make_set(set_rows)
        merged = reconcile_data(txn_df, set_df)
        return detect_issues(merged, txn_df)

    # ── Test 1: Perfect match ─────────────────────────────────
    def test_perfect_match(self):
        """Two transactions, two matching settlements — zero issues."""
        txns = [
            {"txn_id": "T1", "amount": 100.00, "date": "2024-01-01"},
            {"txn_id": "T2", "amount": 200.00, "date": "2024-01-02"},
        ]
        sets = [
            {"settlement_id": "S1", "txn_id": "T1",
             "amount": 100.00, "settlement_date": "2024-01-02"},
            {"settlement_id": "S2", "txn_id": "T2",
             "amount": 200.00, "settlement_date": "2024-01-03"},
        ]
        summary, detail = self._run(txns, sets)
        self.assertEqual(summary["issues_found"], 0)
        self.assertTrue(detail.empty)

    # ── Test 2: Duplicate transaction ─────────────────────────
    def test_duplicate_transaction(self):
        """Duplicate txn_id in transactions should be flagged."""
        txns = [
            {"txn_id": "T1", "amount": 100.00, "date": "2024-01-01"},
            {"txn_id": "T1", "amount": 100.00, "date": "2024-01-01"},  # dup
        ]
        sets = [
            {"settlement_id": "S1", "txn_id": "T1",
             "amount": 100.00, "settlement_date": "2024-01-02"},
        ]
        summary, detail = self._run(txns, sets)
        self.assertGreater(summary["breakdown"]["duplicate_transaction"], 0)
        dup_issues = detail["issues"].explode().tolist()
        self.assertIn("DUPLICATE_TRANSACTION", dup_issues)

    # ── Test 3: Rounding mismatch ─────────────────────────────
    def test_rounding_mismatch(self):
        """Amount diff ≤ $0.02 should be flagged as ROUNDING_MISMATCH."""
        txns = [{"txn_id": "T1", "amount": 99.99, "date": "2024-01-01"}]
        sets = [{"settlement_id": "S1", "txn_id": "T1",
                 "amount": 100.00, "settlement_date": "2024-01-02"}]
        summary, detail = self._run(txns, sets)
        self.assertGreater(summary["breakdown"]["rounding_mismatch"], 0)
        issues = detail["issues"].explode().tolist()
        self.assertIn("ROUNDING_MISMATCH", issues)

    # ── Test 4: Amount mismatch (beyond tolerance) ────────────
    def test_amount_mismatch(self):
        """Amount diff > $0.02 should be flagged as AMOUNT_MISMATCH."""
        txns = [{"txn_id": "T1", "amount": 100.00, "date": "2024-01-01"}]
        sets = [{"settlement_id": "S1", "txn_id": "T1",
                 "amount": 105.00, "settlement_date": "2024-01-02"}]
        summary, detail = self._run(txns, sets)
        self.assertGreater(summary["breakdown"]["amount_mismatch"], 0)

    # ── Test 5: Missing settlement ────────────────────────────
    def test_missing_settlement(self):
        """Transaction with no matching settlement → MISSING_SETTLEMENT."""
        txns = [{"txn_id": "T1", "amount": 100.00, "date": "2024-01-01"}]
        sets: list = []
        summary, detail = self._run(txns, sets)
        self.assertGreater(summary["breakdown"]["missing_settlement"], 0)

    # ── Test 6: Orphan settlement ─────────────────────────────
    def test_orphan_settlement(self):
        """Settlement with txn_id not in transactions → ORPHAN_SETTLEMENT."""
        txns: list = []
        sets = [{"settlement_id": "S1", "txn_id": "T-GHOST",
                 "amount": 50.00, "settlement_date": "2024-01-02"}]
        summary, detail = self._run(txns, sets)
        self.assertGreater(summary["breakdown"]["orphan_settlement"], 0)

    # ── Test 7: Delayed settlement ────────────────────────────
    def test_delayed_settlement(self):
        """Settlement arriving > 3 days later → DELAYED_SETTLEMENT."""
        txns = [{"txn_id": "T1", "amount": 100.00, "date": "2024-01-01"}]
        sets = [{"settlement_id": "S1", "txn_id": "T1",
                 "amount": 100.00, "settlement_date": "2024-02-15"}]   # 45 days
        summary, detail = self._run(txns, sets)
        self.assertGreater(summary["breakdown"]["delayed_settlement"], 0)

    # ── Test 8: Orphan refund ─────────────────────────────────
    def test_orphan_refund(self):
        """Refund whose original_txn_id doesn't exist → ORPHAN_REFUND."""
        txns = [
            {"txn_id": "REF-1", "amount": -50.0, "date": "2024-01-05",
             "type": "refund", "original_txn_id": "TXN-999"},
        ]
        sets = [{"settlement_id": "S1", "txn_id": "REF-1",
                 "amount": -50.0, "settlement_date": "2024-01-06"}]
        summary, detail = self._run(txns, sets)
        self.assertGreater(summary["breakdown"]["orphan_refund"], 0)

    # ── Test 10: Different settlement_ids for same txn_id ────────
    def test_no_false_duplicate_on_txn_id(self):
        """Two distinct settlement_ids that share a txn_id must NOT be
        flagged as DUPLICATE_SETTLEMENT — only identical settlement_ids are dups."""
        txns = [{"txn_id": "T1", "amount": 100.00, "date": "2024-01-01"}]
        # Two different settlement records pointing to the same txn
        # (e.g., a split payout) — different settlement_ids, same txn_id.
        sets = [
            {"settlement_id": "S1", "txn_id": "T1",
             "amount": 60.00, "settlement_date": "2024-01-02"},
            {"settlement_id": "S2", "txn_id": "T1",
             "amount": 40.00, "settlement_date": "2024-01-02"},
        ]
        summary, detail = self._run(txns, sets)
        all_issues = []
        if not detail.empty and "issues" in detail.columns:
            all_issues = detail["issues"].explode().tolist()
        self.assertNotIn("DUPLICATE_SETTLEMENT", all_issues)

    # ── Test 11: True duplicate settlement (same settlement_id) ──
    def test_true_duplicate_settlement(self):
        """Same settlement_id appearing twice IS a real duplicate."""
        txns = [{"txn_id": "T1", "amount": 100.00, "date": "2024-01-01"}]
        sets = [
            {"settlement_id": "S1", "txn_id": "T1",
             "amount": 100.00, "settlement_date": "2024-01-02"},
            {"settlement_id": "S1", "txn_id": "T1",  # exact duplicate
             "amount": 100.00, "settlement_date": "2024-01-02"},
        ]
        summary, detail = self._run(txns, sets)
        self.assertGreater(summary["breakdown"]["duplicate_settlement"], 0)
        all_issues = detail["issues"].explode().tolist()
        self.assertIn("DUPLICATE_SETTLEMENT", all_issues)
    def test_mixed_scenario(self):
        """Multiple issues in one run — ensure all are caught."""
        txns = [
            {"txn_id": "T1", "amount": 100.00, "date": "2024-01-01"},  # ok
            {"txn_id": "T2", "amount": 200.00, "date": "2024-01-02"},  # missing settlement
            {"txn_id": "T3", "amount": 99.99,  "date": "2024-01-03"},  # rounding
            {"txn_id": "T4", "amount":  50.00, "date": "2024-01-04"},  # delayed
        ]
        sets = [
            {"settlement_id": "S1", "txn_id": "T1",
             "amount": 100.00, "settlement_date": "2024-01-02"},
            {"settlement_id": "S3", "txn_id": "T3",
             "amount": 100.00, "settlement_date": "2024-01-04"},
            {"settlement_id": "S4", "txn_id": "T4",
             "amount":  50.00, "settlement_date": "2024-02-20"},  # delayed
            {"settlement_id": "S5", "txn_id": "T-GHOST",
             "amount":  10.00, "settlement_date": "2024-01-03"},  # orphan
        ]
        summary, detail = self._run(txns, sets)
        b = summary["breakdown"]
        self.assertGreater(b["missing_settlement"], 0)
        self.assertGreater(b["rounding_mismatch"],  0)
        self.assertGreater(b["delayed_settlement"],  0)
        self.assertGreater(b["orphan_settlement"],   0)
        self.assertGreater(summary["issues_found"], 0)


# =============================================================
#  6.  ENTRY POINT
# =============================================================

if __name__ == "__main__":
    import sys

    print("\n" + "=" * 60)
    print("  RUNNING UNIT TESTS")
    print("=" * 60)
    loader = unittest.TestLoader()
    suite  = loader.loadTestsFromTestCase(TestReconciliation)
    runner = unittest.TextTestRunner(verbosity=2, stream=sys.stdout)
    result = runner.run(suite)

    print("\n" + "=" * 60)
    print("  RUNNING FULL RECONCILIATION ON SYNTHETIC DATA")
    print("=" * 60 + "\n")
    summary, detail_df = run_reconciliation(verbose=True)

    # Persist outputs
    with open("/mnt/user-data/outputs/reconciliation_summary.json", "w") as f:
        json.dump(summary, f, indent=2)

    if not detail_df.empty:
        detail_df.to_csv(
            "/mnt/user-data/outputs/reconciliation_detail.csv", index=False
        )
        print("\nOutputs saved:")
        print("  → reconciliation_summary.json")
        print("  → reconciliation_detail.csv")
