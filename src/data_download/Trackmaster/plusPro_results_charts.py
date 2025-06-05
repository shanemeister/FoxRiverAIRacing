import os
import requests
from requests.auth import HTTPBasicAuth
from datetime import datetime
import time

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

# Start a session to maintain headers and re-use TCP
session = requests.Session()

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
    return f"http://www.trackmaster.com/nm/tpp_link/download/nm/TM_WEB/{track_code.lower()}{date_str}plusXML.zip"

def download_file(download_url, save_dir, retries=3, delay=5):
    headers = {
        'Referer': 'https://www.trackmaster.com/',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'
    }

    auth = HTTPBasicAuth(USERNAME, PASSWORD)

    for attempt in range(retries):
        try:
            response = session.get(download_url, headers=headers, auth=auth, timeout=30)

            if response.status_code == 200:
                file_name = download_url.split("/")[-1].split('?')[0]
                save_path = os.path.join(save_dir, file_name)
                with open(save_path, 'wb') as file:
                    file.write(response.content)
                print(f"✅ Downloaded: {save_path}")
                return
            elif response.status_code == 403:
                print(f"🚫 Access denied for {download_url} — check subscription.")
            else:
                print(f"❌ Failed to download: {download_url} | Status: {response.status_code}")
                print("Response snippet:", response.text[:200])

        except requests.exceptions.RequestException as e:
            print(f"⚠️ Attempt {attempt + 1} failed: {e}")

        if attempt < retries - 1:
            print(f"⏳ Retrying in {delay} seconds...")
            time.sleep(delay)

    raise Exception(f"❌ Failed to download {download_url} after {retries} attempts")

def download_daily_data():
    """
    Downloads PlusPro and ResultsCharts files for known tracks.
    """
    from datetime import timedelta

    date = datetime.today()  # Or use: datetime.today() - timedelta(days=1)
    tracks = ["EQC", "CCP", "CMR", "EQG", "EQD", "EQJ", "EQF", "EQB", "BCC", "EQA", "EQI", "EQH",
              "KAR", "RAS", "BCD", "BCT", "DUB", "BFF", "EQK", "EQZ", "BCA", "EQN", "EQX",
              "BCB", "EQO", "EQY", "EQR", "EQV", "EQQ", "BCG", "EQS", "EQM", "EQP", "BCE",
              "EQU", "EQT", "EQW", "SWA", "SWD", "SWC", "LSP", "LTD", "LTH",
              "FG", "FAI", "FP", "ABT", "GPR", "LBG", "CTD", "CTM", "MIL", "NP", "STP",
              "OP", "AZD", "DG", "DUN", "RIL", "SAF", "SON", "TUP", "YAV", "DEP", "EP",
              "HST", "KAM", "KIN", "SAN", "SND", "BHP", "FPX", "BMF", "BSR", "FER", "FNO",
              "HOL", "LA", "LRC", "OSA", "OTH", "OTP", "PLN", "SAC", "SLR", "SOL", "SR",
              "STK", "DMR", "GG", "SA", "ARP", "DEL", "WNT", "CRC", "GPW", "HIA", "LEV",
              "OTC", "PMB", "GP", "TAM", "ATH", "GRA", "PMT", "PRM", "BKF", "BOI", "CAS",
              "EMT", "JRM", "ONE", "POD", "RUP", "SDY", "AP", "DUQ", "FAN", "HAW", "SPT",
              "HOO", "IND", "ANF", "EUR", "WDS", "CD", "ELP", "LEX", "RDM", "SRM", "SRR",
              "KD", "KEE", "TP", "DED", "EVD", "BM", "LAD", "GBF", "SUF", "ASD", "RPD",
              "BOW", "GLN", "GN", "MAR", "MON", "SHW", "TIM", "LRL", "PIM", "DET", "GLD",
              "HP", "MPM", "NVD", "PNL", "CBY", "GF", "KSP", "MAF", "MC", "WMF", "YD",
              "BRO", "CHL", "CLM", "MS", "SOP", "STN", "TRY", "CPW", "FAR", "CLS", "ATO",
              "FON", "HPO", "LEG", "LNN", "RKM", "ATL", "FH", "RB", "MED", "MTH", "ALB",
              "LAM", "RUI", "SFE", "SJD", "SRP", "SUN", "ZIA", "ELK", "ELY", "HCF", "WPR",
              "AQU", "BEL", "BAQ", "FL", "GV", "TGD", "SAR", "MVR", "BEU", "BTP", "RD",
              "TDN", "BRD", "WRD", "RP", "AJX", "PIC", "BRN", "GRP", "FPL", "PRV", "TIL",
              "UN", "PEN", "CHE", "MAL", "PHA", "PID", "PRX", "UNI", "WIL", "AIK", "CAM",
              "CHA", "BCF", "MD", "MDA", "MMD", "PW", "HOU", "LS", "GIL", "MAN", "RET",
              "DXD", "LBT", "WBR", "CNL", "FX", "GRM", "ING", "MID", "MOR", "MTP",
              "ODH", "OKR", "SH", "DAY", "EMD", "HAP", "SUD", "WTS", "FMT", "FAX", "CT",
              "MNR", "SHD", "CWF", "ED", "EDR", "SWF", "WYO", "WO", "PM", "WW", "YM",
              "FTP", "FE"]

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

if __name__ == "__main__":
    print("📥 Starting TrackMaster data download job")
    download_daily_data()