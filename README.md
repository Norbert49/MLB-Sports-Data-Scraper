# MLB Game Score Automation Pipeline

## 🚀 Project Overview

This project presents a **robust, automated Python pipeline** designed to scrape daily MLB (Major League Baseball) game scores and matchups directly from [Baseball-Reference.com](https://www.baseball-reference.com/). The collected data is then seamlessly uploaded to a designated Google Sheet, providing a real-time, shareable, and structured dataset for sports analytics, trend tracking, and historical review.

This pipeline exemplifies advanced data mining capabilities, including targeted web scraping, data structuring, and integration with cloud services, making it an ideal solution for clients requiring up-to-date sports data for various analytical purposes.

## ✨ Key Features & Capabilities

Based on the provided output and common sports data needs, this pipeline offers:

* **Automated Daily Game Score Collection:** Efficiently scrapes daily MLB schedules, extracting game scores and team matchups for specified dates or recent periods.
* **Targeted Web Scraping:** Leverages `requests` and `BeautifulSoup4` to intelligently navigate and extract precise data points from complex sports statistics websites (e.g., `baseball-reference.com`).
* **Google Sheets Integration:** Automatically authenticates with the Google Sheets API, creates (if non-existent) and updates a dedicated spreadsheet with the scraped data.
* **Real-time Data Sharing:** Provides a shareable URL to the Google Sheet upon successful upload, enabling instant access to the collected data for team members or further analysis.
* **Structured Output:** Organizes scraped data into clear, readable formats within the Google Sheet, ready for immediate use.
* **Robust Logging:** Comprehensive `INFO` level logging provides detailed insights into the scraping process, including URL fetches, data points collected, and Google Sheets operations.
* **Configurable Parameters:** Easily customizable via `config.json` for spreadsheet naming, scraping years, and other settings.

## 📈 Pipeline Output & Demonstrative Results

The `run_scraper.py` script executes the full pipeline, providing detailed console output and uploading the results directly to Google Sheets.

### Console Log Example:
tarting MLB data collection and Google Sheets upload...

Running for recent games (default mode)...
🔧 Initializing scraper...
2025-07-11 01:59:23,723 - INFO - Google Sheets client initialized successfully
📊 Running data collection pipeline...
2025-07-11 01:59:23,724 - INFO - Attempting to fetch yearly schedule from: https://www.baseball-reference.com/leagues/MLB/2024-schedule.shtml
2025-07-11 01:59:26,168 - INFO - Successfully fetched yearly schedule for 2024.
2025-07-11 01:59:26,169 - INFO - Searching for date heading: 'Thursday, July 11, 2024'
2025-07-11 01:59:26,207 - INFO - Found daily schedule heading for Thursday, July 11, 2024
2025-07-11 01:59:26,210 - INFO - Collected: Seattle Mariners (11) @ Los Angeles Angels (0) | Box: None
... (truncated for brevity - shows collection of 29 games over 2 days) ...
2025-07-11 01:59:30,430 - INFO - New York Yankees (2) @ Tampa Bay Rays (1) | Box: None
2025-07-11 01:59:33,050 - INFO - Opened existing spreadsheet: MLB Data Analysis - Upwork Demo
2025-07-11 01:59:35,640 - INFO - Uploaded 29 scores/matchups to 'Scores'
2025-07-11 01:59:36,524 - INFO - Spreadsheet 'MLB Data Analysis - Upwork Demo' shared as 'reader' with 'anyone'.
2025-07-11 01:59:36,525 - INFO - Scores Spreadsheet URL: https://docs.google.com/spreadsheets/d/1CuZDCK_uCwLiqjdrWjh9U8gWNREx-7edYaaPZRyvKW8/edit#gid=0
... (indicates skipping box score scrape as links were 'None' in this run) ...
2025-07-11 01:59:36,552 - INFO - Pipeline completed successfully

================================================== 📈 PIPELINE RESULTS
✅ Pipeline completed successfully!
📊 Games processed: 0 (Note: This indicates 'games processed for detailed stats', not total collected matchups)
⚾ Batting records: 0
🥎 Pitching records: 0
📝 Lineup records: 0

📋 Data Structure Overview:
Batting stats columns: Player, Team, AB, R, H, RBI, BB, SO, AVG (and more depending on source)
Pitching stats columns: Pitcher, Team, IP, H, R, ER, BB, SO, ERA (and more depending on source)
Lineup columns: Player, Team, Batting Order, Position

### Google Sheets Output Example:

The scraped data (game scores, teams) is uploaded to a Google Sheet. A sample screenshot or a link to a *read-only public example* of the Google Sheet would be highly beneficial here.

**Example Sheet Structure (Screenshot/Description):**

**Sheet Name:** `Scores`
**Columns:** `Date`, `Away_Team`, `Away_Score`, `Home_Team`, `Home_Score`

| Date         | Away_Team         | Away_Score | Home_Team           | Home_Score |
| :----------- | :---------------- | :--------- | :------------------ | :--------- |
| July 11, 2024 | Seattle Mariners  | 11         | Los Angeles Angels  | 0          |
| July 11, 2024 | Atlanta Braves    | 0          | Arizona D'Backs     | 1          |
| July 10, 2024 | Texas Rangers     | 2          | Los Angeles Angels  | 7          |
| ...          | ...               | ...        | ...                 | ...        |

**Live Demo Spreadsheet (Read-Only):**
[https://docs.google.com/spreadsheets/d/1CuZDCK_uCwLiqjdrWjh9U8gWNREx-7edYaaPZRyvKW8/edit#gid=0](https://docs.google.com/spreadsheets/d/1CuZDCK_uCwLiqjdrWjh9U8gWNREx-7edYaaPZRyvKW8/edit#gid=0)
*(**IMPORTANT**: Ensure this link is public read-only and that the sheet reflects current data you're willing to share publicly.)*

## 🛠️ Technology Stack

* **Python 3.x:** Core programming language.
* **Web Scraping:**
    * `requests`: For making HTTP requests to fetch web page content.
    * `BeautifulSoup4` (`bs4`): For parsing HTML and extracting specific data points from the DOM.
    * `lxml`: Fast XML/HTML parser used by BeautifulSoup.
* **Google Sheets API Interaction:**
    * `gspread`: Python API wrapper for Google Sheets.
    * `oauth2client`: For handling OAuth 2.0 authentication with Google APIs.
* **Date/Time Handling:** `python-dateutil`, `pytz` for robust date and timezone operations.
* **Configuration Management:** `json` for loading dynamic settings from `config.json`.
* **Logging:** Python's built-in `logging` module for detailed operational insights.

## 🚀 Quick Start Guide

### Prerequisites

1.  **Python 3.7+** installed.
2.  **Google Cloud Project & Service Account:**
    * Enable the Google Sheets API and Google Drive API for your project.
    * Create a **Service Account** and download its JSON key file (usually named `your-project-name-xyz123.json`). Rename this file to `credentials.json`.
    * Ensure the service account's email address has **Editor access** to the Google Sheet you intend to use. If the sheet doesn't exist, the script will create it, and the service account will automatically be its owner.

### Setup

1.  **Clone the repository:**
    ```bash
    git clone [https://github.com/YourUsername/MLB-Game-Score-Automation.git](https://github.com/YourUsername/MLB-Game-Score-Automation.git)
    cd MLB-Game-Score-Automation
    ```
2.  **Install dependencies:**
    ```bash
    pip install -r requirements.txt
    ```
    *(A `requirements.txt` file listing `pandas`, `numpy`, `requests`, `beautifulsoup4`, `gspread`, `oauth2client`, `lxml`, `python-dateutil`, `pytz` should be present in the repo.)*

3.  **Add your Google Sheets credentials:**
    * Place your `credentials.json` file (downloaded from Google Cloud Console) in the same directory as `run_scraper.py`. **Ensure this file is listed in `.gitignore` and is NOT committed to your public repository!**

4.  **Customize the configuration:**
    * Edit `config.json` to set your preferred Google Sheet name (e.g., `"spreadsheet_name": "MLB Data Analysis - Upwork Demo"`).
    * You can also adjust other scraping parameters like the target year for the schedule.

### Usage

**To scrape recent games and upload data to Google Sheets:**

```bash
python run_scraper.py
The script will automatically:

Initialize the Google Sheets client.

Fetch the MLB schedule for the specified year.

Identify recent game scores.

Upload the collected scores to the designated Google Sheet.

Print a shareable URL to the updated Google Sheet in the console.
```
