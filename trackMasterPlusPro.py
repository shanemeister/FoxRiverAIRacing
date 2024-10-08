import os
import requests

# Create the ./data directory if it doesn't exist
os.makedirs('data', exist_ok=True)

# Create a session object to persist the login session
session = requests.Session()

# Replace with your actual login URL and credentials
login_url = "https://www.trackmaster.com/login"
credentials = {
    'username': 'rshane',
    'password': 'SparkPy24!'
}

# Log in to the website
response = session.post(login_url, data=credentials)

# Check if login was successful
if response.status_code == 200:
    print("Logged in successfully!")
    
    # Now use the session to download the files
    base_url = "https://www.trackmaster.com/track/download/tmu_link/download/"
    file_template = "ajx{}plusxml.zip?LISTING"
    
    # List of dates to download
    dates = ["20241009", "20241010"]
    
    for date in dates:
        file_url = base_url + file_template.format(date)
        response = session.get(file_url)
        
        if response.status_code == 200:
            # Path to save the file in ./data directory
            file_path = os.path.join('data', f"race_data_{date}.zip")
            # Save the file
            with open(file_path, "wb") as f:
                f.write(response.content)
            print(f"Downloaded and saved file for {date} to {file_path}")
        else:
            print(f"Failed to download file for {date}, Status Code: {response.status_code}")
else:
    print(f"Login failed, Status Code: {response.status_code}")