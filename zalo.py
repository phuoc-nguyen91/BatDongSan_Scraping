from seleniumbase import Driver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import time
import logging

class ZaloGroupScraper:
    def __init__(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
        
        # Enhanced driver options
        self.driver = Driver(
            browser="chrome",
            uc=True,
            headless2=False,
            page_load_strategy="normal",  # Changed to normal for more reliable loading
            #maximize_window=True  # Ensure window is maximized
        )
        # Maximizing window after initializing the driver
        self.driver.maximize_window()
        self.wait = WebDriverWait(self.driver, 20)


    def check_login_status(self):
        """Check if user is logged in"""
        try:
            # Multiple possible selectors for checking login status
            login_indicators = [
                "//div[contains(@class, 'avatar')]",
                "//div[contains(@class, 'profile')]",
                "//div[contains(@class, 'chat-list')]",
                "//div[contains(@class, 'left-side')]",
                "//div[contains(@class, 'conversation')]"
            ]
            
            for selector in login_indicators:
                try:
                    element = self.wait_and_find_element(By.XPATH, selector, timeout=5)
                    if element and element.is_displayed():
                        return True
                except:
                    continue
            return False
        except:
            return False

    def wait_and_find_element(self, by, value, timeout=20, retry_count=3):
        for attempt in range(retry_count):
            try:
                element = WebDriverWait(self.driver, timeout).until(
                    EC.presence_of_element_located((by, value))
                )
                if element.is_displayed():
                    return element
            except Exception as e:
                if attempt == retry_count - 1:
                    self.logger.warning(f"Element not found: {value}")
                time.sleep(2)
        return None

    def wait_for_chat_list_load(self, max_retries=8):
        """Improved chat list detection with extended timeout and refined selectors"""
        self.logger.info("Waiting for chat list to load...")

        chat_list_selectors = [
            "//div[contains(@class, 'chat-list')]",
            "//div[contains(@class, 'conv-list')]",
            "//div[contains(@class, 'conversation-list')]",
            "//div[contains(@class, 'left-side')]//div[contains(@class, 'list')]"
        ]

        for attempt in range(max_retries):
            # Check if login status is verified
            if not self.check_login_status():
                self.logger.warning("Login status check failed, retrying...")
                time.sleep(10)
                continue

            for selector in chat_list_selectors:
                try:
                    # Adjusted timeout and detection for list elements
                    element = WebDriverWait(self.driver, 15).until(
                        EC.presence_of_element_located((By.XPATH, selector))
                    )

                    # Check for visible chat items
                    chat_items = self.driver.find_elements(
                        By.XPATH,
                        ".//div[contains(@class, 'conv-item') or contains(@class, 'chat-item')]"
                    )

                    if element.is_displayed() or len(chat_items) > 0:
                        self.logger.info("Chat list detected successfully")
                        return True

                except Exception as e:
                    continue

            self.logger.warning(f"Attempt {attempt + 1}/{max_retries}: Chat list not detected")

            if attempt == 2:
                self.logger.info("Clicking on chat list area...")
                try:
                    self.driver.execute_script(
                        "document.querySelector('.left-side').click()"
                    )
                except:
                    pass

            elif attempt == 3:
                self.logger.info("Refreshing page...")
                self.driver.refresh()
                time.sleep(12)

            elif attempt == 4:
                self.logger.info("Clearing browser data and refreshing...")
                self.driver.delete_all_cookies()
                self.driver.refresh()
                time.sleep(18)

            time.sleep(8)

        return False


    def find_group_element(self, group_name):
        """Enhanced group finding with multiple strategies"""
        selectors = [
            f"//div[contains(@class, 'conv-item-title')]//span[contains(text(), '{group_name}')]",
            f"//div[contains(@class, 'chat-list')]//span[contains(text(), '{group_name}')]",
            f"//div[contains(@class, 'conversation-item')]//span[text()='{group_name}']",
            f"//div[contains(@class, 'left-side')]//span[contains(text(), '{group_name}')]"
        ]
        
        for selector in selectors:
            element = self.wait_and_find_element(By.XPATH, selector)
            if element and element.is_displayed():
                return element
                
        # Try partial match if exact match fails
        for selector in selectors:
            try:
                elements = self.driver.find_elements(By.XPATH, selector)
                for element in elements:
                    if group_name.lower() in element.text.lower() and element.is_displayed():
                        return element
            except:
                continue
                
        return None

    def scrape_group_members(self, group_name):
        try:
            self.logger.info("Accessing Zalo Web...")
            self.driver.get("https://chat.zalo.me/")
            
            # Initial page load wait
            time.sleep(10)
            
            self.logger.info("Waiting for QR code scan...")
            input("Please scan the QR code and press Enter after logging in...")
            
            # Additional wait after login
            time.sleep(10)
            
            if not self.wait_for_chat_list_load():
                raise Exception("Chat list failed to load. Please try again.")
            
            self.logger.info("Chat list loaded successfully. Looking for group...")
            self.scroll_chat_list()
            
            group_element = self.find_group_element(group_name)
            if not group_element:
                self.logger.info("Group not found on first attempt, trying again...")
                self.scroll_chat_list()
                group_element = self.find_group_element(group_name)
            
            if group_element:
                self.logger.info(f"Found group: {group_name}")
                group_element.click()
                # Continue with member scraping...
            else:
                raise Exception(f"Could not find group: {group_name}")
                
        except Exception as e:
            self.logger.error(f"Error occurred: {str(e)}")
            raise
            
    def scroll_chat_list(self, scroll_attempts=8):
        """Enhanced scrolling with better progress detection"""
        try:
            chat_list = self.wait_and_find_element(
                By.XPATH, 
                "//div[contains(@class, 'chat-list') or contains(@class, 'conv-list')]"
            )
            if chat_list:
                self.logger.info("Scrolling through chat list...")
                previous_height = 0
                
                for i in range(scroll_attempts):
                    current_height = self.driver.execute_script(
                        "return arguments[0].scrollHeight", 
                        chat_list
                    )
                    
                    if current_height == previous_height:
                        break
                        
                    self.driver.execute_script(
                        "arguments[0].scrollTop = arguments[0].scrollHeight", 
                        chat_list
                    )
                    time.sleep(2)  # Increased delay
                    previous_height = current_height
                    
        except Exception as e:
            self.logger.error(f"Error while scrolling: {str(e)}")

    def quit(self):
        self.logger.info("Closing browser...")
        self.driver.quit()

if __name__ == "__main__":
    try:
        #group_name = input("Enter Zalo group name to scan: ").strip()#ECOXUAN A-B-C
        group_name = "ECOXUAN A-B-C"
        scraper = ZaloGroupScraper()
        scraper.scrape_group_members(group_name)
    except Exception as e:
        logging.error(f"Script failed: {str(e)}")

        scraper.quit()