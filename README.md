# **Data Engineering ETL Automation for Fashion Trend Analysis**
This project demonstrates an ETL (Extract, Transform, Load) automation pipeline using Directed Acyclic Graphs (DAGs) in Apache Airflow.

---

## 💃 Overview Project
This project automates the ETL (Extract, Transform, Load) pipeline to process data from multiple sources, integrate it, and provide insights. It focuses on analyzing trends in fashion-related data by leveraging Apache Airflow to manage the workflow. The end goal is to predict seasonal trends and generate valuable reports. The trend is collected based on keyword such as ```fur jacket```, ```cardigan```, ```coat```, etc. 

**📂 Directory Layout ** (🔍 where to look at..)
```
ETL-Fashion-Tren-Analysis/  
│  
├── Analysis/         # Data Visualization   
├── DAGs/             # Directory for Airflow DAGs 
├── Privacy Policy/   # Privacy Policy App for Authorization Pinterest Access Token 
└── Script ETL/       # Functions that run on automation 
```

## 🌟 Pipeline & ETL Architecture 
1. Extract
   Data is collected from two sources
   - Web scraping from X (formerly Twitter)
   - API calls to Pinterest
3. Transform
   - Handle X data: CSV data from X is cleaned and converted into time-series format for engagement metrics
   - Handle Pinterest data: JSON data from Pinterest is flattened and structured to extract growth-related insights
5. Load
   The transformed and integrated data is stored in a PostgreSQL database (data warehouse)
7. Analyze and Report
   The data warehouse serves as the foundation for generating trend insights and visualizations to predict upcoming fashion trends

Apache Airflow orchestrates each step of the ETL process through DAGs.

## 👜 Data Sources
The pipeline pulls data from two primary sources:
1. X (formerly Twitter): Data is extracted via web scraping and exported as a CSV file containing user engagement metrics and other relevant attributes.
2. Pinterest: Data is fetched using an API, providing JSON files containing growth trends and user interaction data.
These sources provide complementary datasets for fashion trend analysis.

## ✨ Extract
1. Web Scraping (X): Engagement metrics such as likes, retweets, and comments are collected and saved as a CSV file.
2. API Call (Pinterest): JSON data is retrieved, including attributes related to growth trends and trend behaviors to the respect of time (yearly, monthly, weekly)
   
## ✨ Transform & Integration
1. Cleaning and reformatting
   - CSV Data: Engagement data is cleaned and converted into a time-series format for weekly analysis
   - JSON Data: Nested structures in JSON are flattened, and key-value pairs are extracted for time-series growth metrics
3. Data integration
   - The cleaned datasets are merged into a unified pandas DataFrame with consistent time intervals
   - Columns such as time_weekly, data_tweet, and data_pinterest are created to combine metrics from both sources

## ✨ Load
- The integrated DataFrame is uploaded into a PostgreSQL database serving as the data warehouse.
- The database provides a centralized storage solution for the processed data, enabling efficient querying and further analysis.

---

### Documentation
<!--
- **Documentation**: [View Documentation](https://example.com/documentation)
- **YouTube**: [Watch on YouTube](https://example.com/youtube-video)
 -->
- **Article Post**: [Read the Article Post](https://potreic.medium.com/etl-automation-airflow-d1a5f263e36e)
