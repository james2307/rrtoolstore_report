import pandas as pd
import streamlit as st
from typing import Tuple, Optional, Dict
import io

def process_paytm_data(file) -> pd.DataFrame:
    """Process Paytm CSV data."""
    paytm_df = pd.read_csv(file)
    
    # Clean Paytm Data
    paytm_df = paytm_df.applymap(lambda x: x.strip("'") if isinstance(x, str) else x)
    paytm_df = paytm_df[["transaction_date", "settled_date", "transaction_type", "amount", "commission", "gst"]]
    
    # Convert relevant columns to numeric
    paytm_df["amount"] = pd.to_numeric(paytm_df["amount"], errors="coerce")
    paytm_df["commission"] = pd.to_numeric(paytm_df["commission"], errors="coerce")
    paytm_df["gst"] = pd.to_numeric(paytm_df["gst"], errors="coerce")
    
    # Modify transaction_type
    paytm_df["transaction_type"] = paytm_df["transaction_type"].replace({"ACQUIRING": "payment", "REFUND": "refund"})
    
    # Rename and create columns
    paytm_df["TDate"] = pd.to_datetime(paytm_df["transaction_date"], format="%d-%m-%Y %H:%M:%S")
    paytm_df["Date"] = paytm_df["settled_date"]
    paytm_df["Platform"] = "PayTm"
    paytm_df["Type"] = paytm_df["transaction_type"]
    paytm_df["Order Amt"] = paytm_df["amount"]
    paytm_df["Fee"] = paytm_df["commission"] + paytm_df["gst"]
    
    paytm_df["Debit"] = paytm_df.apply(
        lambda x: x["Order Amt"] - x["Fee"] if x["Type"] == "refund" else 0, axis=1
    )
    paytm_df["Credit"] = paytm_df.apply(
        lambda x: x["Order Amt"] - x["Fee"] if x["Type"] == "payment" else 0, axis=1
    )
    
    # Initialize additional columns
    paytm_df["Order ID"] = None
    paytm_df["Party Information 1"] = None
    paytm_df["Party Information 2"] = None
    paytm_df["Party Information 3"] = None
    
    return paytm_df

def process_razorpay_data(file) -> pd.DataFrame:
    """Process Razorpay CSV data."""
    razorpay_df = pd.read_csv(file)
    
    razorpay_df = razorpay_df[["payment_captured_at", "settled_at", "transaction_entity", 
                               "amount", "fee (exclusive tax)", "tax"]]
    
    # Convert to numeric
    razorpay_df["amount"] = pd.to_numeric(razorpay_df["amount"], errors="coerce")
    razorpay_df["fee (exclusive tax)"] = pd.to_numeric(razorpay_df["fee (exclusive tax)"], errors="coerce")
    razorpay_df["tax"] = pd.to_numeric(razorpay_df["tax"], errors="coerce")
    
    # Rename columns
    razorpay_df["TDate"] = pd.to_datetime(razorpay_df["payment_captured_at"], dayfirst=True, errors="coerce")
    razorpay_df["Date"] = razorpay_df["settled_at"]
    razorpay_df["Platform"] = "Razorpay"
    razorpay_df["Type"] = razorpay_df["transaction_entity"]
    razorpay_df["Order Amt"] = razorpay_df["amount"]
    razorpay_df["Fee"] = razorpay_df["fee (exclusive tax)"] + razorpay_df["tax"]
    
    razorpay_df["Debit"] = razorpay_df.apply(
        lambda x: x["Order Amt"] - x["Fee"] if x["Type"] == "refund" else 0, axis=1
    )
    razorpay_df["Credit"] = razorpay_df.apply(
        lambda x: x["Order Amt"] - x["Fee"] if x["Type"] == "payment" else 0, axis=1
    )
    
    razorpay_df["Order ID"] = None
    razorpay_df["Party Information 1"] = None
    razorpay_df["Party Information 2"] = None
    razorpay_df["Party Information 3"] = None
    
    return razorpay_df

def process_shopify_data(files) -> pd.DataFrame:
    """Process multiple Shopify CSV files."""
    shopify_df = pd.DataFrame()
    
    for file in files:
        if "orders_export" in file.name.lower():
            temp_df = pd.read_csv(file)
            temp_df.columns = temp_df.columns.str.strip()
            temp_df["Paid at"] = pd.to_datetime(temp_df["Paid at"], errors="coerce")
            
            if "Total" in temp_df.columns:
                temp_df = temp_df[temp_df["Total"].notna()]
                temp_df = temp_df[["Name", "Total", "Billing Name", "Billing Street", 
                                 "Billing Company", "Paid at"]]
                shopify_df = pd.concat([shopify_df, temp_df])
    
    return shopify_df.sort_values("Name")

def match_orders(paytm_df: pd.DataFrame, razorpay_df: pd.DataFrame, 
                shopify_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Match orders between payment platforms and Shopify data."""
    paytm_used_names = set()
    razorpay_used_names = set()
    
    def match_order_id_and_party_info(row):
        if row["Type"] == "payment" and row["Order ID"] is None:
            matching_row = shopify_df[
                (shopify_df["Total"] == row["Order Amt"]) &
                (shopify_df["Paid at"].dt.date == row["TDate"].date())
            ]
            if not matching_row.empty:
                for _, shopify_row in matching_row.iterrows():
                    if shopify_row["Name"] not in paytm_used_names:
                        row["Order ID"] = shopify_row["Name"]
                        row["Party Information 1"] = shopify_row["Billing Name"]
                        row["Party Information 2"] = shopify_row["Billing Street"]
                        row["Party Information 3"] = shopify_row["Billing Company"]
                        paytm_used_names.add(shopify_row["Name"])
                        break
        elif row["Type"] == "refund" and row["Order ID"] is None:
            matching_row = shopify_df[
                (shopify_df["Total"] == row["Order Amt"]) &
                (shopify_df["Paid at"].dt.date == row["TDate"].date())
            ]
            if not matching_row.empty:
                for _, shopify_row in matching_row.iterrows():
                    if shopify_row["Name"] not in razorpay_used_names:
                        row["Order ID"] = shopify_row["Name"]
                        row["Party Information 1"] = shopify_row["Billing Name"]
                        row["Party Information 2"] = shopify_row["Billing Street"]
                        row["Party Information 3"] = shopify_row["Billing Company"]
                        razorpay_used_names.add(shopify_row["Name"])
                        break
        return row

    paytm_df = paytm_df.apply(match_order_id_and_party_info, axis=1)
    razorpay_df = razorpay_df.apply(match_order_id_and_party_info, axis=1)
    
    # Find unused Shopify entries
    unused_shopify_df = shopify_df[
        ~shopify_df["Name"].isin(paytm_used_names) & 
        ~shopify_df["Name"].isin(razorpay_used_names)
    ]
    
    return paytm_df, razorpay_df, unused_shopify_df

def combine_and_format_data(paytm_df: pd.DataFrame, razorpay_df: pd.DataFrame) -> pd.DataFrame:
    """Combine and format the final dataset."""
    final_df = pd.concat([paytm_df, razorpay_df])
    final_df = final_df.sort_values(by=["Date", "Platform", "Type", "Order ID"])
    final_df["Sr. No."] = range(1, len(final_df) + 1)
    
    return final_df[["Sr. No.", "Date", "Platform", "Type", "Order Amt", "Fee", 
                     "Debit", "Credit", "Order ID", "Party Information 1", 
                     "Party Information 2", "Party Information 3"]]
