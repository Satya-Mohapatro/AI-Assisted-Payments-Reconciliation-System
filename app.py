import streamlit as st
import pandas as pd
from reconciliation_system import generate_data, reconcile_data, detect_issues

# Page configuration
st.set_page_config(
    page_title="Reconciliation System",
    page_icon="💰",
    layout="wide"
)

# Title and Description
st.title("AI-Assisted Payments Reconciliation System")
st.markdown("Automated matching of business transactions with bank settlements to effortlessly detect discrepancies and anomalies.")

# Initialize session state variables
if "txn_df" not in st.session_state:
    st.session_state.txn_df = None
if "set_df" not in st.session_state:
    st.session_state.set_df = None
if "reconciliation_run" not in st.session_state:
    st.session_state.reconciliation_run = False

# Sidebar Action
with st.sidebar:
    st.header("Controls")
    if st.button("Generate Sample Data", use_container_width=True):
        st.session_state.txn_df, st.session_state.set_df = generate_data()
        st.session_state.reconciliation_run = False
        st.success("Sample data generated successfully!")

# Main App Logic
if st.session_state.txn_df is not None and st.session_state.set_df is not None:
    
    st.header("Datasets")
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Transactions")
        st.dataframe(st.session_state.txn_df, use_container_width=True)
        
    with col2:
        st.subheader("Settlements")
        st.dataframe(st.session_state.set_df, use_container_width=True)
        
    st.divider()

    # Reconciliation Section
    st.header("Reconciliation")
    if st.button("Run Reconciliation", type="primary"):
        merged_df = reconcile_data(st.session_state.txn_df, st.session_state.set_df)
        summary, detail_df = detect_issues(merged_df, st.session_state.txn_df)
        
        st.session_state.summary = summary
        st.session_state.detail_df = detail_df
        st.session_state.reconciliation_run = True

    # Display Results
    if st.session_state.reconciliation_run:
        summary = st.session_state.summary
        detail_df = st.session_state.detail_df
        
        sum_col, issue_col = st.columns([1, 2])
        
        with sum_col:
            st.subheader("Summary")
            st.json(summary)
            
        with issue_col:
            st.subheader("Issues")
            
            if detail_df.empty:
                st.success("✅ Clean match! No issues found in the reconciliation process.")
            else:
                st.warning(f"⚠️ Flagged {summary['issues_found']} records with issues.")
                
                with st.expander("ℹ️ Issue Types Explained", expanded=False):
                    st.markdown("""
                    * **MISSING_SETTLEMENT**: A transaction was recorded, but no corresponding settlement was found.
                    * **ORPHAN_SETTLEMENT**: A settlement arrived from the bank, but no matching transaction exists.
                    * **AMOUNT_MISMATCH**: The transaction amount and settlement amount differ significantly.
                    * **ROUNDING_MISMATCH**: The transaction and settlement amounts differ slightly (within tolerance).
                    * **DELAYED_SETTLEMENT**: The settlement arrived later than the expected date threshold.
                    * **DUPLICATE_...**: Multiple identical records were found for the same ID.
                    * **ORPHAN_REFUND**: A refund was issued for an original transaction that doesn't exist.
                    """)
                
                st.dataframe(detail_df, use_container_width=True)
else:
    st.info("👈 Please click 'Generate Sample Data' in the sidebar to load transactions and settlements.")
