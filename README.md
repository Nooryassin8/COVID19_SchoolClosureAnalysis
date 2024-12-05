
COVID-19 School Closure Response - Power BI Project

Introduction

This project explores the correlation between government-mandated school closures during the COVID-19 pandemic and their impact on confirmed cases and deaths. Using Power BI, we developed an interactive dashboard to analyze and visualize data from multiple sources, offering insights into the relationship between policy responses and pandemic outcomes.

Data Sources

This project relies on two key datasets:

- Bing COVID-19 Dataset: Contains daily updated information on confirmed cases, deaths, and recoveries across all regions as of May 2020.
Government Response Tracker: Documents government policies, including school closure levels, scored as:
  - 0: No measures
  - 1: Recommended closure
  - 2: Required closure (specific levels, e.g., high schools)
  - 3: Required closure of all levels

Challenges

- Identifying datasets that could be effectively correlated.

- Designing the data model (choosing between STAR or Snowflake schema).

- Standardizing data types for seamless integration and measure creation.

Data Processing

- The data processing workflow includes:

Importing Data:

- Imported raw data (CSV files) into SQL Server.

- Created stored procedures for ETL operations.

- Designed reporting views for Power BI consumption.

ETL Steps:

- Connected to the local database.

- Imported datasets (bing_covid-19.csv, GovernmentResponseTracker.csv).

- Created and validated measures and dimension tables.

Backup Management:

- Extracted backup files for secure storage and transfer.

- Imported backups to class servers for demonstration and sharing.

Power BI Dashboard

Features

- Ease of Use: Simple, user-friendly interface for stakeholders.

- Visualization: Incorporates stacked area charts, donut charts, maps, cards, tables, and drill-through features.

- Dynamic Filters: Sync date sliders and bookmarks to allow customized data exploration.

Data Model

- Four dimension tables connected to fact tables in a one-to-many relationship.

- Two fact tables and four measures tables for detailed analysis.

Dashboard Pages

Main Insights:

- Overview of global school closures, confirmed cases, and deaths.

- Comparison of school closure policies and COVID-19 impact by country.

Top 5 States (US):

- Breakdown of school closure levels, confirmed cases, and deaths for top U.S. states.

Drill-Through Pages:

- State-specific insights, such as death rates and closure-to-case ratios.

Administrative Page:

- Metadata, such as row counts for dimension and fact tables and ETL logs.

- Insights and Observations

Global Perspective:

- The U.S. exhibited the highest school closure rate among countries, yet no clear correlation was observed between school closures and reduced cases or deaths.
  
- Countries like South Korea showed low death rates despite minimal school closures.

U.S. Top 5 States:

- California led in full school closures, confirmed cases, and deaths.

- Indiana recorded the highest partial school closures.

Drill-Through Analysis:

- Enabled granular examination of state-level data, including COVID-19 impact and policy responses.

Publishing

- The report was published to the University of Washington's BIDD 330 Spring 2024 platform for broader access and analysis.

Conclusion

- This analysis raises critical questions about the effectiveness of school closures in mitigating the spread of COVID-19. While no definitive correlation was found, the insights provided by this dashboard inform ongoing discussions about public health policy.

How to Use This Repository

- Power BI File: Open the .pbix file to access the dashboard.

Data Sources: Review the data processing steps and raw data files for context.

Insights: Use the interactive visuals to explore correlations and policy effectiveness.

![School Closures ](https://github.com/Nooryassin8/COVID19_SchoolClosureAnalysis/blob/main/Screenshot%202024-12-04%20165255.png)

[TOP 5]  ( https://github.com/Nooryassin8/COVID19_SchoolClosureAnalysis/blob/main/Screenshot%202024-12-04%20165310.png )

[Drill Through] ( https://github.com/Nooryassin8/COVID19_SchoolClosureAnalysis/blob/main/Screenshot%202024-12-04%20165325.png )

[Admin Page] (https://github.com/Nooryassin8/COVID19_SchoolClosureAnalysis/blob/main/Screenshot%202024-12-04%20165336.png)

[Server Info] (https://github.com/Nooryassin8/COVID19_SchoolClosureAnalysis/blob/main/Screenshot%202024-12-04%20165349.png)


