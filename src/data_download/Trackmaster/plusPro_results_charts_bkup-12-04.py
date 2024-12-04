import os
import requests
from urllib.parse import urljoin
from bs4 import BeautifulSoup
from requests.auth import HTTPBasicAuth

# Set the URLs and login credentials
login_url = "https://www.trackmaster.com/myAccountLogin"
pluspro_page_url = "https://www.trackmaster.com/products/tmu/listing"  # URL for PlusPro listings
results_page_url = "https://www.trackmaster.com/products/tch/listing"  # URL for ResultsCharts listings

# Get TrackMaster login credentials from environment variables
USERNAME = os.getenv("TRACKMASTER_USERNAME")
PASSWORD = os.getenv("TRACKMASTER_PASSWORD")

# Ensure that username and password are available
if not USERNAME or not PASSWORD:
    raise Exception("TrackMaster credentials not set in environment variables")

# Define where to save the files
pluspro_dir = '/home/exx/myCode/horse-racing/FoxRiverAIRacing/data/Equibase/PlusPro/Daily'
results_dir = '/home/exx/myCode/horse-racing/FoxRiverAIRacing/data/Equibase/ResultsCharts/Daily'

# Ensure directories exist
os.makedirs(pluspro_dir, exist_ok=True)
os.makedirs(results_dir, exist_ok=True)

# Start a session to maintain login session cookies
session = requests.Session()

def login():
    # Step 1: Fetch the login page to retrieve the CSRF token
    response = session.get(login_url)
    soup = BeautifulSoup(response.text, 'html.parser')

    # Extract the CSRF token from the hidden input field
    csrf_token = soup.find('input', {'name': '_token'})['value']

    # Step 2: Perform login with the CSRF token
    payload = {
        'custom_id': USERNAME,
        'password': PASSWORD,
        '_token': csrf_token  # Include the CSRF token in the POST data
    }

    # Send the POST request to log in
    login_response = session.post(login_url, data=payload)

    # Check if login was successful
    if login_response.status_code == 200 and "My Account" in login_response.text:
        print("Login successful")
    else:
        print("Login failed")
        raise Exception("Failed to log in")

import time

def download_file(download_url, save_dir, retries=3, delay=5):
    headers = {
        'Referer': 'https://www.trackmaster.com/',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3',
    }

    # Use HTTP Basic Auth for each download request
    auth = HTTPBasicAuth(USERNAME, PASSWORD)

    for attempt in range(retries):
        try:
            response = session.get(download_url, headers=headers, auth=auth, timeout=30)

            # Check if the download is successful
            if response.status_code == 200:
                file_name = download_url.split("/")[-1].split('?')[0]
                save_path = os.path.join(save_dir, file_name)
                with open(save_path, 'wb') as file:
                    file.write(response.content)
                print(f"Downloaded: {save_path}")
                return  # Exit after a successful download
            else:
                print(f"Failed to download: {download_url}, Status code: {response.status_code}")
                print("Response headers:", response.headers)

        except requests.exceptions.RequestException as e:
            print(f"Attempt {attempt + 1} failed: {e}")

        if attempt < retries - 1:
            print(f"Retrying in {delay} seconds...")
            time.sleep(delay)

    raise Exception(f"Failed to download {download_url} after {retries} attempts")

def scrape_download_links(page_url, file_extension):
    """
    Scrape the download links from the page URL and return links matching the file extension.
    
    Parameters:
    - page_url: URL of the page to scrape
    - file_extension: File extension to filter download links (e.g., "tch.xml?LISTING" or "plusxml.zip")
    
    Returns:
    - A list of full URLs for the matched files.
    """
    headers = {
        'Referer': 'https://www.trackmaster.com/',  # Adjust if needed
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
    }

    # Send a GET request to the page
    response = session.get(page_url, headers=headers)
    
    if response.status_code == 200:
        soup = BeautifulSoup(response.content, "html.parser")
        # Find all <a> tags with href containing the file_extension
        links = soup.find_all('a', href=True)
        
        # Extract the full URLs for download
        download_links = [
            urljoin("https://www.trackmaster.com", link['href']) 
            for link in links if file_extension in link['href']
        ]
        return download_links
    else:
        print(f"Failed to fetch the page: {page_url}")
        return []

def download_daily_data():
    """
    Function to download daily PlusPro and ResultsCharts data.
    """
    # Scrape and download ResultsCharts XML files
    results_links = scrape_download_links(results_page_url, "tch.xml?LISTING")
    if results_links:
        for link in results_links:
            filename = link.split("/")[-1].split('?')[0]  # Strip query parameters like ?LISTING
            save_path = os.path.join(results_dir, filename)
            download_file(link, results_dir)
    else:
        print("No ResultsCharts download links found.")

    # Scrape and download PlusPro XML files
    pluspro_links = scrape_download_links(pluspro_page_url, "plusxml.zip")
    if pluspro_links:
        for link in pluspro_links:
            filename = link.split("/")[-1].split('?')[0]  # Strip query parameters
            save_path = os.path.join(pluspro_dir, filename)
            download_file(link, pluspro_dir)
    else:
        print("No PlusPro download links found.")

if __name__ == "__main__":
    # Step 1: Login to TrackMaster
    login()
    
    # Step 2: Download daily files
    download_daily_data()