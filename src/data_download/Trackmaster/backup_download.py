import os
import requests
from urllib.parse import urljoin
from bs4 import BeautifulSoup
from requests.auth import HTTPBasicAuth
import time
from datetime import datetime

def get_results_url(track_code, date):
    """
    Build the ResultsCharts XML file URL for a specific track and date.
    """
    date_str = date.strftime('%Y%m%d')  # e.g., 20240517
    return f"https://www.trackmaster.com/cgi-bin/tch_link/download/{track_code.lower()}{date_str}tch.xml"

def get_pluspro_url(track_code, date):
    """
    Build the PlusPro zip file URL for a specific track and date.
    """
    date_str = date.strftime('%Y%m%d')
    return f"http://www.trackmaster.com/nm/tpp_link/download/nm/TM_WEB/{track_code.lower()}{date_str}ppsXML.zip"

def download_daily_data():
    """
    Downloads PlusPro and ResultsCharts files for known tracks.
    """
    from datetime import datetime, timedelta

    date = datetime.today()  # or datetime.today() - timedelta(days=1) for yesterday
    tracks = ["aqu", "wo", "cd", "kee"]  # add others as needed

    for track in tracks:
        results_url = get_results_url(track, date)
        pluspro_url = get_pluspro_url(track, date)

        try:
            download_file(results_url, results_dir)
        except Exception as e:
            print(f"[ERROR] ResultsCharts download failed for {track}: {e}")

        try:
            download_file(pluspro_url, pluspro_dir)
        except Exception as e:
            print(f"[ERROR] PlusPro download failed for {track}: {e}")
            


# Set the URLs and login credentials
login_url = "https://www.trackmaster.com/myAccountLogin"
pluspro_page_url = "https://www.trackmaster.com/products/tmu/listing"  # URL for PlusPro listings
results_page_url = "https://www.trackmaster.com/products/tch/listing"  # URL for ResultsCharts listings


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
    csrf_input = soup.find('input', {'name': '_token'})
    if csrf_input:
        csrf_token = csrf_input['value']
        print(f"CSRF Token: {csrf_token}")
    else:
        print("CSRF token not found on the login page.")
        raise Exception("Failed to retrieve CSRF token")

    # Step 2: Perform login with the CSRF token
    payload = {
        'username': USERNAME,
        'password': PASSWORD,
        '_token': csrf_token  # if this field is required — verify if present
    }

    headers = {
        'Referer': login_url,
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
    }

    # Send the POST request to log in
    login_response = session.post(login_url, data=payload, headers=headers)

    # Debugging output
    print("Login response status code:", login_response.status_code)
    print("Login response URL:", login_response.url)
    print("Login response text snippet:", login_response.text[:500])

    # Check if login was successful
    if login_response.status_code == 200 and "My Account" in login_response.text:
        print("Login successful")
    else:
        print("Login failed")
        raise Exception("Failed to log in")

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
                print("Response content snippet:", response.text[:200])  # For debugging

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
    - file_extension: File extension to filter download links (e.g., "tch.xml" or "plusxml.zip")

    Returns:
    - A list of full URLs for the matched files.
    """
    headers = {
        'Referer': 'https://www.trackmaster.com/',
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
    results_links = scrape_download_links(results_page_url, "tch.xml")
    if results_links:
        for link in results_links:
            download_file(link, results_dir)
    else:
        print("No ResultsCharts download links found.")

    # Scrape and download PlusPro XML files
    pluspro_links = scrape_download_links(pluspro_page_url, "plusxml.zip")
    if pluspro_links:
        for link in pluspro_links:
            download_file(link, pluspro_dir)
    else:
        print("No PlusPro download links found.")

if __name__ == "__main__":
    # Step 1: Login to TrackMaster
    login()

    # Step 2: Download daily files
    download_daily_data()