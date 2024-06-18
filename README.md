# Mentorship_project


## Overview

This project is part of a mentorship program aimed at providing hands-on experience with data analysis and SQL. The goal of the project is to analyze various aspects of sales data, customer feedback, inventory management, and vendor performance to derive actionable insights. 

## Teamwork Pipeline

Our team followed a structured pipeline to ensure efficient collaboration and high-quality results. The pipeline consisted of the following stages:

1. **Planning**:
    - Defined the scope of the project.
    - Identified key objectives and deliverables.
    - Assigned roles and responsibilities to each team member.

2. **Data Collection**:
    - Extracted data from the `AdventureWorks2022` database.
    - Ensured data accuracy and consistency.
    - Stored the data in a structured format for analysis.

3. **Data Analysis**:
    - Utilized SQL queries to analyze different aspects of the data.
    - Created Python scripts for additional data processing and visualization.
    - Employed statistical models to forecast future sales trends.

4. **Review and Feedback**:
    - Conducted peer reviews of the analysis and code.
    - Incorporated feedback to improve the quality of the deliverables.
    - Ensured the final analysis meets the project objectives.

5. **Documentation and Reporting**:
    - Documented the entire analysis process.
    - Prepared a comprehensive report with findings and recommendations.
    - Presented the results to stakeholders.

## Project2: Data Analysis Script

The `project2.py` script is a core component of our analysis pipeline. It performs several critical functions:

### Data Analysis Objectives

- **Sales Performance**:
    - Analyzed total sales by product, month, and year.
    - Identified best-selling and worst-selling products.
    - Evaluated sales performance across different territories.

- **Customer Feedback**:
    - Created and managed a `CustomerFeedback` table.
    - Inserted, updated, and deleted sample feedback records.
    - Analyzed customer feedback to identify areas of improvement.

- **Inventory Management**:
    - Identified products frequently out-of-stock or overstocked.
    - Analyzed inventory levels and sales counts.

- **Vendor Performance**:
    - Calculated average lead times from vendors.
    - Evaluated vendor reliability and cost-effectiveness.

- **Forecasting**:
    - Used ARIMA models to forecast future sales for the next 12 months.

### Key Features of `project2.py`

- **SQL Queries**:
    - Extensive use of SQL to extract and manipulate data from the `AdventureWorks2022` database.
    - Dynamic queries to cater to various analysis requirements.

- **Python Integration**:
    - Leveraged Python's `pandas` library for data manipulation.
    - Utilized `matplotlib` and `seaborn` for data visualization.
    - Implemented statistical models using `statsmodels` for sales forecasting.

- **Error Handling and Logging**:
    - Robust error handling to ensure the script executes smoothly.
    - Comprehensive logging to track the script's progress and catch any issues.

### How to Run the Script

1. **Clone the Repository**:
    ```bash
    git clone https://github.com/HaticeMelikzade/Mentorship_project.git
    cd Mentorship_project
    ```

2. **Install Dependencies**:
    Ensure you have the necessary Python libraries installed:
    ```bash
    pip install pandas matplotlib seaborn statsmodels pyodbc sqlalchemy
    ```

3. **Execute the Script**:
    Run the script to perform the data analysis:
    ```bash
    python project2.py
    ```

4. **View the Results**:
    - The script will generate various visualizations and log files.
    - Check the output directory for graphs and the log file for detailed execution information.

## Conclusion

This project exemplifies effective teamwork and the application of data analysis techniques to derive meaningful insights from complex datasets. By following a structured pipeline and leveraging powerful tools like SQL and Python, we were able to achieve our project objectives and deliver valuable recommendations.

For any questions or further information, please contact the project team.

---

