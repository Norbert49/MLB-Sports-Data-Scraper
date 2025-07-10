# MLB Sports Data Scraper

## Overview
The MLB Sports Data Scraper is a Python application designed to scrape Major League Baseball (MLB) game data, including box scores and lineups, and export the results to Google Sheets. The application utilizes various libraries for web scraping, data manipulation, and Google Sheets integration.

## Features
- Scrapes recent MLB game data from Baseball Reference.
- Extracts batting and pitching statistics.
- Uploads data to Google Sheets.
- Configurable logging for monitoring and debugging.

## Requirements
To run this project, you need to have Python installed along with the following dependencies:

- pandas
- numpy
- requests
- beautifulsoup4
- gspread
- python-dotenv

You can install the required packages using pip:

```
pip install -r requirements.txt
```

## Configuration
Before running the scraper, you need to set up your Google Sheets credentials. Create a `.env` file in the project root directory and add the following environment variables:

```
GOOGLE_SHEETS_CREDENTIALS='{"type": "service_account", "project_id": "your_project_id", "private_key_id": "your_private_key_id", "private_key": "-----BEGIN PRIVATE KEY-----\nYOUR_PRIVATE_KEY\n-----END PRIVATE KEY-----\n", "client_email": "your_service_account_email", "client_id": "your_client_id", "auth_uri": "https://accounts.google.com/o/oauth2/auth", "token_uri": "https://oauth2.googleapis.com/token", "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs", "client_x509_cert_url": "your_client_x509_cert_url"}'
```

Replace the placeholders with your actual Google Sheets API credentials.

## Running the Scraper
To run the scraper, execute the following command in your terminal:

```
python pipeline.py
```

You can specify the number of days back to scrape recent games by modifying the `days_back` parameter in the `run_pipeline` method.

## Logging
The application logs its activities to a file named `mlb_scraper.log`. You can check this file for detailed information about the scraping process and any errors that may occur.

## License
This project is licensed under the MIT License. Feel free to modify and use it as per your requirements.