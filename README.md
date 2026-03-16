# Petrol Price Analysis Project ⛽

## Overview
This project is a **Data Engineering & Analysis pipeline** built to fetch international petrol and diesel prices from multiple sources, analyze data quality and identify the **most reliable data source**.  

We collected data from **three sources**:  

1. **API** – Programmatically fetched petrol prices using the World Bank API.  
2. **Web Scraping** – Scraped live petrol prices from GlobalPetrolPrices.com using BeautifulSoup.  
3. **Manual CSV** – A synthetic fallback dataset curated manually.  

The datasets are analyzed using **pandas** and visualized using **matplotlib**, comparing them across key metrics:  

- **Completeness** – Percentage of target countries with data.  
- **Freshness** – Percentage of data updated in the last 7 days.  
- **Coverage** – How complete each column/field is.  
- **Reliability** – Overall average score combining completeness, freshness, and coverage.  

---
## Results Example

| Source       | Completeness | Freshness | Coverage | Reliability |
|--------------|-------------|-----------|----------|------------|
| API          | 50.00%      | 0.00%     | 87.50%   | 45.83%     |
| Manual CSV   | 50.00%      | 100.00%   | 62.50%   | 70.83%     |
| Web Scraper  | 50.00%      | 100.00%   | 61.25%   | 70.42%     |

> Based on reliability, **Manual CSV** and **Web Scraper** were the most reliable sources in this analysis.

---
## Visualisation

Below is a snapshot showing **the workflow and analysis results**.  

<img width="1717" height="458" alt="Screenshot 2026-03-16 221413" src="https://github.com/user-attachments/assets/093b7816-80bf-46c8-b0ec-b97f4f216970" />

---

## Results & Insights

- The analysis evaluated petrol price datasets based on **Completeness, Freshness, Coverage, and Reliability**.  
- Overall reliability shows that **Manual CSV** and **Web Scraper** are the most dependable sources for accurate petrol price data.  
- The API dataset, while official, had lower freshness, making it less reliable for up-to-date analysis.

