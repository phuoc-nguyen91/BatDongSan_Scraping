# streamlit_app.py
import streamlit as st
import pandas as pd
from datetime import datetime
import os
from scraper import PropertyScraper
from gcs_module import GCSModule
import io

def get_current_time_str():
    current_datetime = datetime.now()
    formatted_datetime = current_datetime.strftime("%d_%m_%Y_%H_%M")
    return formatted_datetime

def convert_df_to_csv(df):
    buffer = io.StringIO()
    buffer.write('\ufeff')
    df.to_csv(buffer, index=False, encoding='utf-8')
    return buffer.getvalue()

def main():
    st.title("Batdongsan.com.vn Web Scraper")
    
    # GCS credentials configuration
    gcs_json_path = st.text_input(
        "Path to Google Cloud credentials JSON file (optional)",
        help="Leave empty to skip Google Cloud Storage upload"
    )
    bucket_name = st.text_input("GCS Bucket name (optional)")
    
    # Initialize GCS if credentials provided
    gcs_module = None
    if gcs_json_path and bucket_name and os.path.exists(gcs_json_path):
        try:
            gcs_module = GCSModule(bucket_name, gcs_json_path)
            st.success("Successfully connected to Google Cloud Storage")
        except Exception as e:
            st.error(f"Failed to connect to GCS: {str(e)}")
    
    # Input for URLs
    urls_input = st.text_area(
        "Enter URLs to scrape (one per line)",
        help="Enter the URLs of the pages you want to scrape, one URL per line"
    )
    
    num_threads = st.number_input(
        "Number of threads",
        min_value=1,
        max_value=5,
        value=2
    )
    
    max_pages = st.number_input(
        "Maximum number of pages per URL (optional)",
        min_value=1,
        value=5,
        help="Leave blank or set to a higher number to scrape all available pages"
    )
    
    if st.button("Start Scraping"):
        if not urls_input.strip():
            st.error("Please enter at least one URL")
            return
        
        urls = [url.strip() for url in urls_input.strip().split('\n') if url.strip()]
        
        with st.spinner("Scraping data..."):
            try:
                # Create progress bar
                progress_bar = st.progress(0)
                status_text = st.empty()
                
                scraper = PropertyScraper(gcs_module)
                df = scraper.scrape_properties(
                    urls,
                    num_threads=num_threads,
                    max_pages=max_pages
                )
                
                st.success("Scraping completed!")
                
                # Show summary statistics
                st.subheader("Summary")
                st.write(f"Total properties scraped: {len(df)}")
                
                # Display the data
                st.subheader("Scraped Data")
                st.dataframe(df)
                
                # Convert dataframe to CSV with proper encoding
                csv = convert_df_to_csv(df)
                
                # Download button for CSV
                st.download_button(
                    label="Download CSV",
                    data=csv,
                    file_name=f"properties_{get_current_time_str()}.csv",
                    mime="text/csv"
                )
                
                # Show storage information
                if gcs_module:
                    st.info(f"Data has been saved to Google Cloud Storage bucket: {bucket_name}")
                st.info(f"Data has been saved locally in: scraped_data/properties_{get_current_time_str()}.csv")
                
            except Exception as e:
                st.error(f"An error occurred: {str(e)}")

if __name__ == "__main__":
    main()