import streamlit as st
import pandas as pd
from data_processor import (process_paytm_data, process_razorpay_data, 
                          process_shopify_data, match_orders, combine_and_format_data)
from utils import validate_csv_file, get_download_link

import hmac

st.set_page_config(
    page_title="Transaction Data Processor",
    page_icon="💳",
    layout="wide"
)

def check_password():
    """Returns `True` if the user had the correct password."""

    def password_entered():
        """Checks whether a password entered by the user is correct."""
        if hmac.compare_digest(st.session_state["password"], st.secrets["password"]):
            st.session_state["password_correct"] = True
            del st.session_state["password"]  # Don't store the password.
        else:
            st.session_state["password_correct"] = False

    # Return True if the password is validated.
    if st.session_state.get("password_correct", False):
        return True

    # Show input for password.
    st.text_input(
        "Password", type="password", on_change=password_entered, key="password"
    )
    if "password_correct" in st.session_state:
        st.error("😕 Password incorrect")
    return False

if not check_password():
    st.stop()  # Do not continue if check_password is not True.

# Main Streamlit app starts here
st.write("Here goes your normal Streamlit app...")
st.button("Click me")

def main():

    # Initialize session state
    if 'processed_data' not in st.session_state:
        st.session_state.processed_data = None
    if 'unused_data' not in st.session_state:
        st.session_state.unused_data = None
    if 'files_processed' not in st.session_state:
        st.session_state.files_processed = False

    st.title("Transaction Data Processor")
    st.markdown("""
    This application processes and combines transaction data from PayTm, Razorpay, and Shopify.
    Upload your CSV files below to get started.
    """)

    # Instructions expander
    with st.expander("📋 Click here to view instructions for downloading reports"):
        st.markdown("""
        ### PayTM Instructions
        1. Visit [PayTM Dashboard](https://dashboard.paytmpayments.com/) and login.
        2. Navigate to: **Dashboards → Reports & Invoices → Reports**
        3. Select "**Settlements (Historical upto 24 months)**"
        4. Choose duration:
           - From: 1st of previous month
           - To: End of previous month
        5. Click "Generate Report"
        6. Wait for the report to be generated and download it
        7. Upload all the files downloaded under "PayTM Data"

        ### Razorpay Instructions
        1. Visit [Razorpay Dashboard](https://dashboard.razorpay.com/) and login
        2. Click **Reports**
        3. Click **Downloads**
        4. Find the "Shopify Settlement Report" for the last month
        5. Download the report
        6. Upload all the files downloaded under "Razorpay Data"

        ### Shopify Instructions
        1. Check the previous month's report and note the last Order ID
        2. Visit [Shopify Admin](https://admin.shopify.com/) and login
        3. Click **Orders**
        4. Download all Order ID data (post the last Order ID from the previous month)
        5. **Important Note**: Download page by page (50 orders at a time)
           - Downloading multiple pages at once will try to send an email to support@rrtoolstore.com
           - Due to some issues, that email is not received
           - Download each page one by one for automatic download to PC
        6. To download:
           - Click checkbox (near "Order")
           - Click Export
           - Select "Export as Plain CSV File"
           - Click "Export Orders"
        7. Upload all the files downloaded under "Shopify Data"

        ### Understanding the Output Files
        1. **Processed Data**:
           - This report needs to be shared over email
           - Some Order IDs might be missing and need manual addition
           - Mismatches generally occur due to refunds

        2. **Unmatched Shopify Entries**:
           - Contains Shopify Order IDs uploaded but not used in Processed Data
           - Helps find missing entries in Processed Data
           - Use for cross-checking Order IDs directly on Shopify panel
           - Cross-reference details like amount, etc.
        """)

    # File upload section
    st.header("File Upload")
    col1, col2, col3 = st.columns(3)

    with col1:
        st.subheader("PayTm Data")
        paytm_file = st.file_uploader("Upload PayTm CSV", type=['csv'], key="paytm")

    with col2:
        st.subheader("Razorpay Data")
        razorpay_file = st.file_uploader("Upload Razorpay CSV", type=['csv'], key="razorpay")

    with col3:
        st.subheader("Shopify Data")
        shopify_files = st.file_uploader("Upload Shopify CSVs", 
                                      type=['csv'], 
                                      accept_multiple_files=True,
                                      key="shopify")

    process_clicked = st.button("Process Data")

    # Check if we need to process the data
    if process_clicked or (not st.session_state.files_processed and all([paytm_file, razorpay_file, shopify_files])):
        with st.spinner("Processing data..."):
            try:
                # Process individual datasets
                paytm_df = process_paytm_data(paytm_file)
                razorpay_df = process_razorpay_data(razorpay_file)
                shopify_df = process_shopify_data(shopify_files)

                # Match orders
                processed_paytm_df, processed_razorpay_df, unused_shopify_df = match_orders(
                    paytm_df, razorpay_df, shopify_df
                )

                # Combine data
                final_df = combine_and_format_data(processed_paytm_df, processed_razorpay_df)

                # Store in session state
                st.session_state.processed_data = final_df
                st.session_state.unused_data = unused_shopify_df
                st.session_state.files_processed = True

            except Exception as e:
                st.error(f"An error occurred during processing: {str(e)}")
                st.session_state.files_processed = False
                return

    # Display results if data is processed
    if st.session_state.files_processed and st.session_state.processed_data is not None:
        st.header("Results")

        # Summary statistics
        final_df = st.session_state.processed_data
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total Transactions", len(final_df))
        with col2:
            st.metric("Total Credits", f"₹{final_df['Credit'].sum():,.2f}")
        with col3:
            st.metric("Total Debits", f"₹{final_df['Debit'].sum():,.2f}")

        # Preview data
        st.subheader("Preview of Processed Data")
        st.dataframe(final_df.head())

        # Download buttons
        col1, col2 = st.columns(2)
        with col1:
            processed_file, processed_name = get_download_link(
                final_df, "processed_transactions.csv"
            )
            st.download_button(
                "Download Processed Data",
                processed_file,
                processed_name,
                "text/csv",
                key='download_processed'
            )

        with col2:
            if st.session_state.unused_data is not None:
                unused_file, unused_name = get_download_link(
                    st.session_state.unused_data, "unused_shopify_entries.csv"
                )
                st.download_button(
                    "Download Unmatched Shopify Entries",
                    unused_file,
                    unused_name,
                    "text/csv",
                    key='download_unused'
                )

if __name__ == "__main__":
    main()
