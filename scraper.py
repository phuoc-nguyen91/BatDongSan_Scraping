# scraper.py
from seleniumbase import Driver
from threading import Thread
from bs4 import BeautifulSoup
from datetime import datetime
import pandas as pd
import json
import logging
import os
import re
import time

class PropertyScraper:
    def __init__(self, gcs_module=None):
        self.gcs = gcs_module
        
    def create_chrome_driver(self, num_chrome):
        chrome_drivers = []
        for _ in range(num_chrome):
            driver = Driver(uc_cdp=True, incognito=True, block_images=True, headless=True)
            driver.set_page_load_timeout(30)
            chrome_drivers.append(driver)
        return chrome_drivers

    def extract_coordinates(self, html_content):
        pattern = r'place\?q=([-+]?\d*\.\d+),([-+]?\d*\.\d+)'
        match = re.search(pattern, html_content)
        if match:
            latitude = float(match.group(1))
            longitude = float(match.group(2))
            return [latitude, longitude]
        else:
            return [None, None]

    def safe_find(self, soup, selector, class_name, default=None):
        try:
            element = soup.find(selector, class_=class_name)
            return element.text.strip() if element else default
        except Exception:
            return default

    def get_pagination_urls(self, base_url, max_pages=None):
        """Get URLs for all pages"""
        try:
            driver = Driver(uc_cdp=True, incognito=True, block_images=True, headless=True)
            driver.get(base_url)
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            
            # Find the last page number
            pagination = soup.find('div', class_='re__pagination')
            if pagination:
                page_items = pagination.find_all('a', class_='re__pagination-number')
                if page_items:
                    try:
                        last_page = max([int(item.text) for item in page_items if item.text.isdigit()])
                    except ValueError:
                        last_page = 1
                else:
                    last_page = 1
            else:
                last_page = 1
                
            driver.quit()

            # Limit the number of pages if specified
            if max_pages:
                last_page = min(last_page, max_pages)

            # Generate URLs for all pages
            urls = []
            for page in range(1, last_page + 1):
                if page == 1:
                    urls.append(base_url)
                else:
                    if '/p' in base_url:
                        # If URL already has a page number, replace it
                        urls.append(re.sub(r'/p\d+', f'/p{page}', base_url))
                    else:
                        # Add page number before any query parameters
                        if '?' in base_url:
                            base, params = base_url.split('?', 1)
                            urls.append(f"{base}/p{page}?{params}")
                        else:
                            urls.append(f"{base_url}/p{page}")
            
            return urls
        except Exception as e:
            logging.error(f"Error getting pagination URLs: {str(e)}")
            return [base_url]  # Return only the base URL if something goes wrong

    def process_single_property(self, property_url, chrome_driver):
        # [Previous process_single_property code remains the same]
        try:
            chrome_driver.get(property_url)
            time.sleep(1)  # Reduced delay to 1 second
            html_content = chrome_driver.page_source
            soup = BeautifulSoup(html_content, 'html.parser')
            
            property_data = {
                "Diện tích": None, "Mức giá": None,
                "Mặt tiền": None, "Đường vào": None,
                "Hướng nhà": None, "Hướng ban công": None,
                "Số tầng": None, "Số phòng ngủ": None,
                "Số toilet": None, "Pháp lý": None,
                "Nội thất": None, "Ngày đăng": None,
                "Ngày hết hạn": None, "Loại tin": None,
                "Mã tin": None, "Địa chỉ": None,
                "latitude": None, "longitude": None,
                "url": property_url
            }

            specs_div = soup.find('div', class_='re__pr-specs-content js__other-info')
            if specs_div:
                titles = specs_div.find_all('span', class_='re__pr-specs-content-item-title')
                values = specs_div.find_all('span', class_='re__pr-specs-content-item-value')
                
                for title, value in zip(titles, values):
                    property_data[title.get_text().strip()] = value.get_text().strip()

            address = self.safe_find(soup, 'span', 're__pr-short-description js__pr-address')
            if address:
                property_data["Địa chỉ"] = address

            map_div = soup.find('div', class_='re__section re__pr-map js__section js__li-other')
            if map_div:
                coords = self.extract_coordinates(str(map_div))
                property_data["latitude"] = coords[0]
                property_data["longitude"] = coords[1]

            short_info = soup.find('div', class_='re__pr-short-info re__pr-config js__pr-config')
            if short_info:
                info_titles = short_info.find_all('span', class_='title')
                info_values = short_info.find_all('span', class_='value')
                
                for title, value in zip(info_titles, info_values):
                    property_data[title.get_text().strip()] = value.get_text().strip()

            return property_data

        except Exception as e:
            logging.error(f"Error processing property {property_url}: {str(e)}")
            return None

    def scrape_properties(self, base_urls, num_threads=2, max_pages=None):
        all_page_urls = []
        for base_url in base_urls:
            all_page_urls.extend(self.get_pagination_urls(base_url, max_pages))

        chrome_drivers = self.create_chrome_driver(num_threads)
        results = []
        
        def worker(driver, url_list):
            for url in url_list:
                try:
                    driver.get(url)
                    time.sleep(1)
                    html_content = driver.page_source
                    soup = BeautifulSoup(html_content, 'html.parser')
                    property_urls = ['https://batdongsan.com.vn' + element.get('href') 
                                   for element in soup.select('.js__product-link-for-product-id')]
                    
                    for property_url in property_urls:
                        property_data = self.process_single_property(property_url, driver)
                        if property_data:
                            results.append(property_data)
                            
                except Exception as e:
                    logging.error(f"Error processing {url}: {str(e)}")
                    continue

        # Split URLs among threads
        url_chunks = [all_page_urls[i::num_threads] for i in range(num_threads)]
        threads = []
        
        for i, chunk in enumerate(url_chunks):
            t = Thread(target=worker, args=(chrome_drivers[i], chunk))
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        for driver in chrome_drivers:
            try:
                driver.quit()
            except Exception:
                pass

        # Convert results to DataFrame
        df = pd.DataFrame(results)
        
        # Save to CSV locally
        os.makedirs("scraped_data", exist_ok=True)
        timestamp = datetime.now().strftime("%d_%m_%Y_%H_%M")
        local_path = f"scraped_data/properties_{timestamp}.csv"
        
        # Save with UTF-8-BOM encoding
        with open(local_path, 'w', encoding='utf-8-sig') as f:
            df.to_csv(f, index=False)
        
        if self.gcs:
            try:
                csv_content = df.to_csv(index=False, encoding='utf-8-sig')
                self.gcs.upload_file_to_bucket(
                    csv_content,
                    f"scraped_data/properties_{timestamp}.csv"
                )
            except Exception as e:
                logging.error(f"Error uploading to GCS: {str(e)}")
        
        return df