# 📰 Alternative Data Engineering: News Scraping & NLP for Public Policy

![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Web Scraping](https://img.shields.io/badge/Web_Scraping-BeautifulSoup_%7C_Selenium-43B02A?style=for-the-badge)
![NLP](https://img.shields.io/badge/NLP-Sentiment_%7C_Polarity-FF9E0F?style=for-the-badge)
![Data Engineering](https://img.shields.io/badge/Data_Engineering-ETL_Pipelines-005b9f?style=for-the-badge)

## 📌 Project Overview
In modern economic analysis, official macroeconomic indicators often suffer from severe publication lags. This repository presents an **Alternative High-Frequency Data Pipeline** designed to monitor public policy, political sentiment, and economic shocks in real-time across the Andean region (Bolivia & Ecuador).

This project focuses heavily on **Data Engineering and Natural Language Processing (NLP)**. It features native, modularized web scrapers built from scratch (>1,000 lines of Python code) to extract, harmonize, and process mass volumes of press articles, transforming unstructured text into structured datasets for econometric or sentiment analysis.

## ⚙️ Core Architecture & ETL Pipeline
This repository is organized into a robust ETL (Extract, Transform, Load) workflow:

1. **Extraction (Web Scraping Modules):**
   - 🇧🇴 **Bolivia 24h Tracker:** A highly optimized script designed for daily execution, scraping contemporary news updates within a 24-hour window from major Bolivian news outlets.
   - 🇪🇨 **Ecuador Historical Scraper:** A fully modularized architecture (orchestrator + 3 specialized modules) capable of extracting historical news archives from Ecuadorian media for any specific target date.
2. **Transformation (NLP Engine):**
   - Text normalization, tokenization, and stop-word removal.
   - Calculation of polarity and sentiment scores.
   - Entity mention tracking and word-cloud generation for political/economic framing.

## 📂 Directory Structure

```text
public-policy-news-scraping/
├── Data/
│   ├── Raw/                    # Raw JSON/CSV exports from web scrapers
│   └── Cleaned/                # Tokenized and harmonized text datasets
├── Scrapers/                   # Python extraction modules
│   ├── Bolivia_24h/            # Scripts for daily contemporary scraping
│   └── Ecuador_Historical/     # Modularized historical scraping orchestrator
├── NLP_Analysis/               
│   └── 01_sentiment_polarity_eda.ipynb  # Jupyter Notebook: Sentiment & Polarity Analysis
└── requirements.txt            # Python dependencies (BeautifulSoup, pandas, nltk, etc.)
```

## ⚖️ Ethical Scraping Statement
All extraction modules in this repository are designed strictly adhering to **ethical web scraping guidelines**:
- Respect for `robots.txt` directives of the target news outlets.
- Implementation of automated delays (`time.sleep()`) and rate-limiting to prevent server overload (Denial of Service).
- Identification via proper `User-Agent` headers.

## 📊 Mapping of Exhibits (NLP Outputs)

| Exhibit | Description | Generating Script | Output File |
|---------|-------------|-------------------|-------------|
| **Figure 1** | Media Polarity Distribution (Ecuador vs Bolivia) | `01_sentiment_polarity_eda.ipynb` | `Outputs/Figures/polarity_dist.png` |
| **Figure 2** | Political/Economic Word Cloud | `01_sentiment_polarity_eda.ipynb` | `Outputs/Figures/wordcloud.png` |

## 💻 Computational Requirements
- **Python:** Version 3.9+ 
- **Required Libraries:** `beautifulsoup4`, `requests`, `pandas`, `nltk` (or `spacy`), `wordcloud`, `matplotlib`, `seaborn`.
- **Execution:** The Ecuador historical orchestrator may require extended wall-clock time depending on the target date range and enforced server rate limits.

---
*Created by [Juan José Bedregal](https://github.com/juanbedregal-code)*
