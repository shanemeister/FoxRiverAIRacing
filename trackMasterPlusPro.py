from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import StaleElementReferenceException, TimeoutException
from webdriver_manager.chrome import ChromeDriverManager
import os
import time
import re

# Specify the directory where you want to save the downloaded files
download_dir = os.path.join(os.getcwd(), 'data')

# Ensure the directory exists
if not os.path.exists(download_dir):
    os.makedirs(download_dir)

# Set up Chrome options
chrome_options = Options()
chrome_options.add_argument("--headless")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--disable-gpu")

# Add preferences to allow download in headless mode
chrome_prefs = {
    "download.default_directory": download_dir,
    "profile.default_content_settings.popups": 0,
    "directory_upgrade": True,
    "download.prompt_for_download": False,
    "safebrowsing.enabled": True
}

chrome_options.add_experimental_option("prefs", chrome_prefs)

# Automatically manage ChromeDriver using webdriver-manager
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)

# Navigate to the login page
driver.get('https://www.trackmaster.com/myAccountLogin')

try:
    # Wait until the login form is present
    username_field = WebDriverWait(driver, 20).until(
        EC.presence_of_element_located((By.ID, "custom_id"))
    )
    password_field = driver.find_element(By.ID, 'password')

    # Enter login credentials
    username_field.send_keys("rshane")  # Change to your actual username
    password_field.send_keys("SparkPy2024")  # Change to your actual password

    # Click the login button
    login_button = driver.find_element(By.ID, 'glSubmit')
    login_button.click()

    # Wait for the login to complete
    WebDriverWait(driver, 20).until(EC.url_contains("myAccount"))

    # Navigate to the listing page after login
    driver.get('https://www.trackmaster.com/products/tmu/listing')

    # Regular expression to match the required files
    pattern = re.compile(r".*plusxml\.zip\?LISTING$")

    # Filter and download the files that match the pattern
    while True:
        try:
            download_links = WebDriverWait(driver, 20).until(
                EC.presence_of_all_elements_located((By.TAG_NAME, "a"))
            )
            for link in download_links:
                href = link.get_attribute('href')
                print(f"Found link: {href}")  # Print the URL being processed
                if href and pattern.match(href):
                    try:
                        file_name = href.split('/')[-1].split('?')[0]  # Extract filename
                        print(f"Downloading file: {file_name}")
                        
                        # Use JavaScript to click the download link
                        driver.execute_script("arguments[0].click();", link)

                        # Sleep for a few seconds to allow the download to complete
                        time.sleep(10)  # Increase sleep time to ensure download completes

                        # Confirm the file has been downloaded by checking the directory
                        if file_name in os.listdir(download_dir):
                            print(f"Successfully downloaded {file_name}")
                        else:
                            print(f"Failed to download {file_name}")
                            # Check if the file is in the default download directory
                            default_download_dir = os.path.expanduser('~/Downloads')
                            if file_name in os.listdir(default_download_dir):
                                print(f"File found in default download directory: {default_download_dir}")
                            else:
                                print(f"File not found in default download directory: {default_download_dir}")
                    except Exception as e:
                        print(f"Error downloading file: {e}")
            break
        except StaleElementReferenceException:
            print("Encountered StaleElementReferenceException, retrying...")
            continue
        except TimeoutException:
            print("Encountered TimeoutException, retrying...")
            continue

finally:
    driver.quit()