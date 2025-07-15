# ⚾ MLB Sports Data Scraper: Interview Presentation

This project showcases an automated Python pipeline for comprehensive MLB data collection. It addresses the technical test requirements by demonstrating skills in data scraping, cleaning, organization, exporting to both CSV and Google Sheets, and generating AI-powered game insights.

---

## 🎯 Test Task Requirements & Solutions

### ✅ 1. Game Selection

- **Automated Recent Games**: Scrapes games from the past N days (configurable).
- **Specific Game URL**: Accepts direct box score links from [Baseball-Reference.com](https://www.baseball-reference.com/).

### ✅ 2. Data Gathering

#### A. Box Score Data (from Baseball-Reference.com)
- **Final Score**: Home & Away.
- **Batting Stats**: AB, R, H, RBI, BB, SO, AVG, OBP, SLG, OPS.
- **Pitching Stats**: IP, H, R, ER, BB, SO, ERA, HR.
- **Game Info**: Venue, Attendance, Duration, Umpires, Weather, Field Condition, Start Time, Winning/Losing/Saving Pitchers.

#### B. Starting Lineups
- Player names, batting order, and field positions.

#### C. Betting Odds (via [The Odds API](https://the-odds-api.com))
- **Moneyline Odds**.
- **Point Spreads and Totals**.
- Multiple bookmakers supported.

### ✅ 3. Data Export

#### Google Sheets (Primary Output)
Organized into clear, separate tabs:
- `Batting Stats`
- `Pitching Stats`
- `Lineup Info`
- `Game Info`
- `Betting Odds`
- `Daily Scores`
- `Game Insights` (new!)

#### Clean CSV Files (Secondary Output)
All datasets are also exported to the `output/` folder as CSVs, including AI-generated insights.

### ✅ 4. AI-Powered Game Insights (Short Notes)
Uses a Large Language Model (LLM) to:
- Spot trends, anomalies, underdog wins.
- Highlight player performances.
- Summarize game dynamics.
- Provide actionable notes in the `Game Insights` tab.

---

## 📊 Data Pipeline Overview

```mermaid
graph TD
    A[Configuration<br>(config.json)] --> B(Google Cloud<br>Service Account)
    A --> C(MLB Data Scraping Pipeline)
    B --> C
    C --> D{1. Identify Games<br>(Recent/Test URL)}
    D --> E[GameScraper<br>(Box Scores & Info)]
    E --> F{2. Fetch Betting Odds}
    F --> G[OddsScraper<br>(The Odds API)]
    G --> H{3. Aggregate & Clean}
    E --> H
    H --> I[DataExporter<br>CSV & GSheet Upload]
    I --> J[MLBInsightsGenerator<br>LLM-Driven Insights]
    I --> K[4. Export]
    J --> K
    K --> L[CSV Output]
    K --> M[Google Sheets]
