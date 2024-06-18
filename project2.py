#%% Import necessary libraries
import pyodbc
import sqlalchemy
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
import logging
from datetime import datetime
from statsmodels.tsa.arima.model import ARIMA

# Set up logging
log_filename = f"log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[logging.FileHandler(log_filename), 
                              logging.StreamHandler()])

# Log the start of the script
logging.info("Script started.")

#%% Define functions for executing SQL queries and uploading data

def get_engine(server, database):
    connection_string = f'mssql+pyodbc://{server}/{database}?trusted_connection=yes&driver=ODBC+Driver+17+for+SQL+Server'
    return sqlalchemy.create_engine(connection_string)

def execute_sql(query, server, database):
    try: 
        logging.info("Attempting to connect to the database for executing SQL.")
        engine = get_engine(server, database)
        with engine.connect() as conn:
            conn.execute(sqlalchemy.text(query))
        logging.info("SQL query executed successfully.")
    except Exception as e:
        logging.error(f"Error executing query: {e}")
        print(f"Error executing query: {e}")

def upload_data(dataframe, table, server, database, upload_type='replace'):
    try:
        logging.info("Attempting to connect to the database for uploading data.")
        engine = create_engine(
            f'mssql+pyodbc://{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server'
        )
        logging.info(f"Uploading data to table: {table}")

        # Exclude 'FeedbackID' column if it exists
        if 'FeedbackID' in dataframe.columns:
            dataframe = dataframe.drop(columns=['FeedbackID'])

        # Upload data to table
        dataframe.to_sql(
            name=table,
            con=engine,
            if_exists=upload_type,
            index=False,
            schema="dbo",
            chunksize=10000
        )
        
        logging.info(f"Data uploaded successfully to {table}.")

    except Exception as e:
        logging.error(f"Error uploading data: {e}")
        print(f"Error uploading data: {e}")

def retrieve_data(query, server, database):
    try:
        logging.info("Attempting to connect to the database for retrieving data.")
        conn = pyodbc.connect(
            f'Driver={{ODBC Driver 17 for SQL Server}};'
            f'Server={server};'
            f'Database={database};'
            'Trusted_Connection=yes;'
        )
        
        logging.info(f"Retrieving data with query: {query}")
        
        # Read data using SQL query
        df = pd.read_sql(query, conn)
        
        logging.info("Data retrieved successfully.")
        return df
        
    except Exception as e:
        logging.error(f"Error retrieving data: {e}")
        print(f"Error retrieving data: {e}")
        return pd.DataFrame()  
    finally:
        if conn:
            conn.close()

# Define parameters
server = "DESKTOP-KHTL27T" 
database = "AdventureWorks2022"

################################################

#%% Query to get the total sales per product for orders placed in the last year
query_last_year_sales = """
SELECT
    p.Name AS ProductName,
    SUM(sod.LineTotal) AS TotalSales
FROM
    Sales.SalesOrderDetail sod
JOIN
    Sales.SalesOrderHeader soh ON sod.SalesOrderID = soh.SalesOrderID
JOIN
    Production.Product p ON sod.ProductID = p.ProductID
WHERE
    soh.OrderDate >= DATEADD(year, -10, GETDATE())
GROUP BY
    p.Name
ORDER BY
    TotalSales DESC;
"""

# Retrieve data for last year's sales
df_last_year_sales = retrieve_data(query_last_year_sales, server, database)
logging.info("Last Year Sales Data:\n%s", df_last_year_sales.head())

# Check if DataFrame is not empty, then export to CSV
if not df_last_year_sales.empty:
    df_last_year_sales.to_csv("lastYearSales.csv", index=False)
    logging.info("Data exported to lastYearSales.csv")
else:
    logging.info("No sales data available for last year.")

    
# Plotting the data
if not df_last_year_sales.empty:
    plt.figure(figsize=(12, 6))
    plt.bar(df_last_year_sales['ProductName'], df_last_year_sales['TotalSales'], color='skyblue')
    plt.xlabel('Product Name')
    plt.ylabel('Total Sales')
    plt.title('Total Sales per Product for Orders Placed in the Last Year')
    plt.xticks(rotation=90)  # Rotate the x labels for better visibility
    plt.tight_layout()  # Adjust layout to make room for rotated labels
    plt.show()
else:
    logging.info("No sales data available to plot.")

###############################################################################
#%%  Read the CSV file

csv_file_path = r'C:\Users\Melikzade\Desktop\SQL Course Materials\productReviews'
try:
    df_reviews = pd.read_csv(csv_file_path)
    logging.info(f"CSV file {csv_file_path} read successfully.")
except Exception as e:
    logging.error(f"Error reading CSV file: {e}")

# Create a new table in the database
create_table_query = """
IF OBJECT_ID('dbo.ProductReviews', 'U') IS NOT NULL 
   DROP TABLE dbo.ProductReviews;
CREATE TABLE dbo.ProductReviews (
    ID INT IDENTITY PRIMARY KEY,
    product_id NVARCHAR(255),
    product_parent NVARCHAR(255),
    product_title NVARCHAR(255),
    star_rating INT,
    helpful_votes INT,
    total_votes INT,
    vine NVARCHAR(10),
    verified_purchase NVARCHAR(10),
    review_headline NVARCHAR(255),
    review_body NVARCHAR(MAX),
    review_date DATE
);
"""

execute_sql(create_table_query, server, database)
def upload_data(df, table_name, server, database):
    try:
        engine = get_engine(server, database)
        df.to_sql(table_name, con=engine, index=False, if_exists='replace')
        logging.info(f"Data uploaded successfully to {table_name}.")
    except SQLAlchemyError as e:
        logging.error(f"Error uploading data: {e}")



####################################################################
#%% Create a new table called CustomerFeedback if it does not exist
create_table_query = """
IF OBJECT_ID('dbo.CustomerFeedback', 'U') IS NOT NULL 
   DROP TABLE dbo.CustomerFeedback;
CREATE TABLE dbo.CustomerFeedback (
  FeedbackID INT PRIMARY KEY IDENTITY,
  CustomerID INT NOT NULL,
  FeedbackDate DATE NOT NULL,
  Comments VARCHAR(300)
);
"""
try:
    execute_sql(create_table_query, server, database)
    logging.info("CustomerFeedback table created successfully.")
    print("CustomerFeedback table created successfully.")
except Exception as e:
    logging.error(f"Failed to create CustomerFeedback table: {e}")
    print(f"Failed to create CustomerFeedback table: {e}")

#%% Insert sample data into CustomerFeedback table
query_insert_sample_data = """
INSERT INTO dbo.CustomerFeedback (CustomerID, FeedbackDate, Comments) 
VALUES 
    (1001, '2022-09-01', 'Great service!'),
    (1002, '2022-09-05', 'Product quality is excellent'),
    (1003, '2022-09-10', 'Could improve packaging'),
    (1001, '2022-09-12', 'Fast delivery');
"""
try:
    execute_sql(query_insert_sample_data, server, database)
    logging.info("Sample data inserted into CustomerFeedback table successfully.")
    print("Sample data inserted into CustomerFeedback table successfully.")
except Exception as e:
    logging.error(f"Failed to insert sample data into CustomerFeedback table: {e}")
    print(f"Failed to insert sample data into CustomerFeedback table: {e}")

#%% Update a record in CustomerFeedback table
query_update_customer_feedback = """
UPDATE dbo.CustomerFeedback
SET Comments = 'Great customer support'
WHERE FeedbackID = 3;
"""
try:
    execute_sql(query_update_customer_feedback, server, database)
    logging.info("CustomerFeedback table updated successfully.")
    print("CustomerFeedback table updated successfully.")
except Exception as e:
    logging.error(f"Failed to update CustomerFeedback table: {e}")
    print(f"Failed to update CustomerFeedback table: {e}")

#%% Delete a record from CustomerFeedback table
query_delete_customer_feedback = """
DELETE FROM dbo.CustomerFeedback
WHERE FeedbackID = 2;
"""
try:
    execute_sql(query_delete_customer_feedback, server, database)
    logging.info("Record deleted from CustomerFeedback table successfully.")
    print("Record deleted from CustomerFeedback table successfully.")
except Exception as e:
    logging.error(f"Failed to delete record from CustomerFeedback table: {e}")
    print(f"Failed to delete record from CustomerFeedback table: {e}")

#%% TOTAL SALES BY MONTH AND YEAR
query_total_sales_year = """
SELECT YEAR(OrderDate) AS Year, SUM(TotalDue) AS TotalSales
FROM Sales.SalesOrderHeaderSalesReason
GROUP BY YEAR(OrderDate)
ORDER BY Year;
"""
try:
    df_total_sales_year = retrieve_data(query_total_sales_year, server, database)
    logging.info("Total Sales by Year Data:\n%s", df_total_sales_year.head())
    print("Total Sales by Year Data retrieved successfully.")
except Exception as e:
    logging.error(f"Failed to retrieve Total Sales by Year data: {e}")
    print(f"Failed to retrieve Total Sales by Year data: {e}")

query_monthly_sales = """
SELECT YEAR(OrderDate) AS Year, MONTH(OrderDate) AS Month, SUM(TotalDue) AS TotalSales
FROM Sales.SalesOrderHeader
GROUP BY YEAR(OrderDate), MONTH(OrderDate)
ORDER BY Year, Month;
"""
try:
    df_monthly_sales = retrieve_data(query_monthly_sales, server, database)
    logging.info("Monthly Sales Trends:\n%s", df_monthly_sales.head())
    print("Monthly Sales Trends data retrieved successfully.")
except Exception as e:
    logging.error(f"Failed to retrieve Monthly Sales Trends data: {e}")
    print(f"Failed to retrieve Monthly Sales Trends data: {e}")

# Combine Year and Month into a single datetime column for plotting
df_monthly_sales['Date'] = pd.to_datetime(df_monthly_sales[['Year', 'Month']].assign(day=1))

# Plot total sales by year
plt.figure(figsize=(10, 6))
sns.barplot(x='Year', y='TotalSales', data=df_monthly_sales, palette='viridis')
plt.title('Total Sales by Year')
plt.xlabel('Year')
plt.ylabel('Total Sales')
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# Retrieve data for total sales by month and year
query_monthly_sales = """
SELECT YEAR(OrderDate) AS Year, MONTH(OrderDate) AS Month, SUM(TotalDue) AS TotalSales
FROM Sales.SalesOrderHeader
GROUP BY YEAR(OrderDate), MONTH(OrderDate)
ORDER BY Year, Month;
"""
try:
    df_monthly_sales = retrieve_data(query_monthly_sales, server, database)
    logging.info("Total Sales by Month and Year Data:\n%s", df_monthly_sales.head())
    print("Total Sales by Month and Year data retrieved successfully.")
except Exception as e:
    logging.error(f"Failed to retrieve Total Sales by Month and Year data: {e}")
    print(f"Failed to retrieve Total Sales by Month and Year data: {e}")

df_monthly_sales['Date'] = pd.to_datetime(df_monthly_sales[['Year', 'Month']].assign(day=1))

# Plot total sales by month and year
plt.figure(figsize=(14, 7))
sns.lineplot(x='Date', y='TotalSales', data=df_monthly_sales, marker='o')
plt.title('Total Sales by Month and Year')
plt.xlabel('Date')
plt.ylabel('Total Sales')
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

###########################################################################################
#%% BEST AND WORST SELLING PRODUCTS
sales_analysis_query = """
SELECT 
    p.ProductID,
    p.Name AS ProductName,
    SUM(sod.LineTotal) AS TotalSales
FROM 
    Sales.SalesOrderDetail sod
JOIN 
    Production.Product p ON sod.ProductID = p.ProductID
GROUP BY 
    p.ProductID, p.Name
ORDER BY 
    TotalSales DESC;
"""
try:
    df_sales_analysis = retrieve_data(sales_analysis_query, server, database)
    logging.info("Sales Analysis - Best and Worst-Selling Products:\n%s", df_sales_analysis.head())
    print("Sales Analysis data retrieved successfully.")
except Exception as e:
    logging.error(f"Failed to retrieve Sales Analysis data: {e}")
    print(f"Failed to retrieve Sales Analysis data: {e}")

# Plotting best and worst-selling products
plt.figure(figsize=(12, 8))

# Plot top 10 best-selling products
top_10_best_selling = df_sales_analysis.head(10)

plt.subplot(2, 1, 1)
sns.barplot(x='TotalSales', y='ProductName', data=top_10_best_selling, palette='viridis')
plt.title('Top 10 Best-Selling Products')
plt.xlabel('Total Sales')
plt.ylabel('Product Name')

# Plot top 10 worst-selling products (assuming worst-selling means the last 10 products)
top_10_worst_selling = df_sales_analysis.tail(10)

plt.subplot(2, 1, 2)
sns.barplot(x='TotalSales', y='ProductName', data=top_10_worst_selling, palette='viridis')
plt.title('Top 10 Worst-Selling Products')
plt.xlabel('Total Sales')
plt.ylabel('Product Name')

plt.tight_layout()
plt.show()

########################################################################################################
#%% Segment customers based on purchase behavior and demographics
customer_segmentation_query = """
SELECT 
    c.CustomerID,
    a.City,
    a.StateProvinceID,
    COUNT(soh.SalesOrderID) AS OrderCount,
    SUM(soh.TotalDue) AS TotalSpent
FROM 
    Sales.Customer c
INNER JOIN 
    Sales.SalesOrderHeader soh ON c.CustomerID = soh.CustomerID
INNER JOIN 
    Person.Address a ON soh.BillToAddressID = a.AddressID
GROUP BY 
    c.CustomerID, a.City, a.StateProvinceID
ORDER BY 
    TotalSpent DESC;
"""
try:
    df_customer_segmentation = retrieve_data(customer_segmentation_query, server, database)
    logging.info("Customer Segmentation Data:\n%s", df_customer_segmentation.head())
    print("Customer Segmentation data retrieved successfully.")
except Exception as e:
    logging.error(f"Failed to retrieve Customer Segmentation data: {e}")
    print(f"Failed to retrieve Customer Segmentation data: {e}")

# Let's plot TotalSpent versus OrderCount
plt.figure(figsize=(14, 7))
sns.scatterplot(x='OrderCount', y='TotalSpent', data=df_customer_segmentation, hue='StateProvinceID', palette='viridis')
plt.title('Customer Segmentation - Total Spent vs Order Count')
plt.xlabel('Order Count')
plt.ylabel('Total Spent')
plt.legend(title='State/Province', loc='upper right', bbox_to_anchor=(1.15, 1))
plt.tight_layout()
plt.show()

# Let's also visualize TotalSpent across different states
plt.figure(figsize=(14, 7))
state_avg_spent = df_customer_segmentation.groupby('StateProvinceID')['TotalSpent'].mean().reset_index()

sns.barplot(x='StateProvinceID', y='TotalSpent', data=state_avg_spent, palette='viridis')
plt.title('Average Total Spent by State/Province')
plt.xlabel('State/Province ID')
plt.ylabel('Average Total Spent')
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

#############################################################################################################

#%% Customer Demographic Analysis
query_customer_demo = """
SELECT 
    c.CustomerID,
    a.City,
    a.StateProvinceID,
    COUNT(soh.SalesOrderID) AS OrderCount,
    SUM(soh.TotalDue) AS TotalSpent
FROM 
    Sales.Customer c
INNER JOIN 
    Sales.SalesOrderHeader soh ON c.CustomerID = soh.CustomerID
INNER JOIN 
    Person.Address a ON soh.BillToAddressID = a.AddressID
GROUP BY 
    c.CustomerID, a.City, a.StateProvinceID
ORDER BY 
    TotalSpent DESC;
"""
try:
    df_customer_demo = retrieve_data(query_customer_demo, server, database)
    logging.info("Customer Demographic Analysis Data:\n%s", df_customer_demo.head())
    print("Customer Demographic Analysis data retrieved successfully.")
except Exception as e:
    logging.error(f"Failed to retrieve Customer Demographic Analysis data: {e}")
    print(f"Failed to retrieve Customer Demographic Analysis data: {e}")

# Scatter plot: OrderCount vs. TotalSpent, colored by StateProvinceID
plt.figure(figsize=(14, 7))
sns.scatterplot(x='OrderCount', y='TotalSpent', hue='StateProvinceID', data=df_customer_demo, palette='viridis')
plt.title('Customer Demographic Analysis - Total Spent vs Order Count by State')
plt.xlabel('Order Count')
plt.ylabel('Total Spent')
plt.legend(title='State/Province ID', bbox_to_anchor=(1.05, 1), loc='upper left')
plt.tight_layout()
plt.show()

# Bar plot: Average TotalSpent by StateProvinceID
plt.figure(figsize=(14, 7))
state_avg_spent = df_customer_demo.groupby('StateProvinceID')['TotalSpent'].mean().reset_index()
sns.barplot(x='StateProvinceID', y='TotalSpent', data=state_avg_spent, palette='viridis')
plt.title('Average Total Spent by State/Province')
plt.xlabel('State/Province ID')
plt.ylabel('Average Total Spent')
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# Bar plot: Average OrderCount by StateProvinceID
plt.figure(figsize=(14, 7))
state_avg_orders = df_customer_demo.groupby('StateProvinceID')['OrderCount'].mean().reset_index()
sns.barplot(x='StateProvinceID', y='OrderCount', data=state_avg_orders, palette='viridis')
plt.title('Average Order Count by State/Province')
plt.xlabel('State/Province ID')
plt.ylabel('Average Order Count')
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# Count plot: Number of Customers per StateProvinceID
plt.figure(figsize=(14, 7))
sns.countplot(x='StateProvinceID', data=df_customer_demo, palette='viridis')
plt.title('Number of Customers by State/Province')
plt.xlabel('State/Province ID')
plt.ylabel('Number of Customers')
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# Count plot: Number of Customers per City
# We will plot only the top 20 cities by number of customers to avoid clutter
top_20_cities = df_customer_demo['City'].value_counts().head(20).index.tolist()
plt.figure(figsize=(14, 7))
sns.countplot(y='City', data=df_customer_demo[df_customer_demo['City'].isin(top_20_cities)], order=top_20_cities, palette='viridis')
plt.title('Number of Customers by City (Top 20 Cities)')
plt.xlabel('Number of Customers')
plt.ylabel('City')
plt.tight_layout()
plt.show()

##########################################################################################################################################
#%% Analyze customer retention and factors influencing retention
customer_retention_query = """
SELECT 
    c.CustomerID,
    COUNT(DISTINCT soh.SalesOrderID) AS RepeatPurchaseCount,
    SUM(soh.TotalDue) AS TotalSpent,
    MAX(soh.OrderDate) AS LatestPurchaseDate
FROM 
    Sales.Customer c
INNER JOIN 
    Sales.SalesOrderHeader soh ON c.CustomerID = soh.CustomerID
GROUP BY 
    c.CustomerID 
ORDER BY 
    RepeatPurchaseCount DESC;
"""
try:
    df_customer_retention = retrieve_data(customer_retention_query, server, database)
    logging.info("Customer Retention Analysis Data:\n%s", df_customer_retention.head())
    print("Customer Retention Analysis data retrieved successfully.")
except Exception as e:
    logging.error(f"Failed to retrieve Customer Retention Analysis data: {e}")
    print(f"Failed to retrieve Customer Retention Analysis data: {e}")

# Plot 1: Distribution of Repeat Purchases
plt.figure(figsize=(10, 6))
plt.hist(df_customer_retention['RepeatPurchaseCount'], bins=20, edgecolor='k', alpha=0.7)
plt.title('Distribution of Repeat Purchases')
plt.xlabel('Number of Repeat Purchases')
plt.ylabel('Number of Customers')
plt.grid(True)
plt.show()

# Plot 2: Total Amount Spent by Customers
plt.figure(figsize=(10, 6))
plt.hist(df_customer_retention['TotalSpent'], bins=20, edgecolor='k', alpha=0.7)
plt.title('Total Amount Spent by Customers')
plt.xlabel('Total Amount Spent')
plt.ylabel('Number of Customers')
plt.grid(True)
plt.show()

# Plot 3: Latest Purchase Date
plt.figure(figsize=(10, 6))
df_customer_retention['LatestPurchaseDate'] = pd.to_datetime(df_customer_retention['LatestPurchaseDate'])
plt.hist(df_customer_retention['LatestPurchaseDate'], bins=20, edgecolor='k', alpha=0.7)
plt.title('Latest Purchase Date Distribution')
plt.xlabel('Date of Latest Purchase')
plt.ylabel('Number of Customers')
plt.grid(True)
plt.show()

######################################################################################

#%% Identify products frequently out-of-stock or overstocked
inventory_management_query = """
SELECT 
    p.ProductID,
    p.Name AS ProductName,
    pi.Quantity AS InventoryQty,
    COUNT(sod.SalesOrderDetailID) AS SalesCount
FROM 
    Production.Product p
JOIN 
    Production.ProductInventory pi ON p.ProductID = pi.ProductID
LEFT JOIN 
    Sales.SalesOrderDetail sod ON p.ProductID = sod.ProductID
GROUP BY 
    p.ProductID, p.Name, pi.Quantity
HAVING 
    (pi.Quantity = 0 AND COUNT(sod.SalesOrderDetailID) = 0) OR pi.Quantity > 100
ORDER BY 
    InventoryQty ASC, SalesCount DESC;
"""
try:
    df_inventory_management = retrieve_data(inventory_management_query, server, database)
    logging.info("Inventory Management Analysis Data:\n%s", df_inventory_management.head())
    print("Inventory Management Analysis data retrieved successfully.")
except Exception as e:
    logging.error(f"Failed to retrieve Inventory Management Analysis data: {e}")
    print(f"Failed to retrieve Inventory Management Analysis data: {e}")

###############################################

#%% Calculate average lead times from vendors
average_lead_time_query = """
SELECT 
    pv.BusinessEntityID AS VendorID,
    v.Name AS VendorName,
    AVG(pv.AverageLeadTime) AS AvgLeadTimeDays
FROM 
    Purchasing.ProductVendor pv
JOIN 
    Purchasing.Vendor v ON pv.BusinessEntityID = v.BusinessEntityID
GROUP BY 
    pv.BusinessEntityID, v.Name
ORDER BY 
    AvgLeadTimeDays ASC;
"""
try:
    df_avg_lead_time = retrieve_data(average_lead_time_query, server, database)
    logging.info("Average Lead Time Analysis Data:\n%s", df_avg_lead_time.head())
    print("Average Lead Time Analysis data retrieved successfully.")
except Exception as e:
    logging.error(f"Failed to retrieve Average Lead Time Analysis data: {e}")
    print(f"Failed to retrieve Average Lead Time Analysis data: {e}")

#####################################################################

#%% Evaluate vendor reliability and cost-effectiveness
vendor_evaluation_query = """
SELECT 
    pv.BusinessEntityID AS VendorID,
    v.Name AS VendorName,
    COUNT(po.PurchaseOrderID) AS PurchaseOrders,
    AVG(po.TotalDue) AS AvgOrderCost,
    AVG(DATEDIFF(day, po.OrderDate, po.ShipDate)) AS AvgDeliveryDay
FROM 
    Purchasing.Vendor v
JOIN 
    Purchasing.ProductVendor pv ON v.BusinessEntityID = pv.BusinessEntityID
JOIN 
    Purchasing.PurchaseOrderHeader po ON pv.ProductID = po.PurchaseOrderID
GROUP BY 
    pv.BusinessEntityID, v.Name
ORDER BY 
    PurchaseOrders DESC, AvgOrderCost ASC, AvgDeliveryDay ASC;
"""
try:
    df_vendor_evaluation = retrieve_data(vendor_evaluation_query, server, database)
    logging.info("Vendor Evaluation Data:\n%s", df_vendor_evaluation.head())
    print("Vendor Evaluation data retrieved successfully.")
except Exception as e:
    logging.error(f"Failed to retrieve Vendor Evaluation data: {e}")
    print(f"Failed to retrieve Vendor Evaluation data: {e}")

#####################################################
#%% Profitability of individual products
product_profitability_query = """
SELECT 
    p.ProductID,
    p.Name AS ProductName,
    SUM(sod.LineTotal) AS TotalSales,
    SUM(sod.UnitPrice * sod.OrderQty) AS TotalCost,
    SUM(sod.LineTotal) - SUM(sod.UnitPrice * sod.OrderQty) AS Profit
FROM 
    Production.Product p
JOIN 
    Sales.SalesOrderDetail sod ON p.ProductID = sod.ProductID
GROUP BY 
    p.ProductID, p.Name
ORDER BY 
    Profit DESC;
"""
try:
    df_product_profitability = retrieve_data(product_profitability_query, server, database)
    logging.info("Product Profitability Analysis Data:\n%s", df_product_profitability.head())
    print("Product Profitability Analysis data retrieved successfully.")
except Exception as e:
    logging.error(f"Failed to retrieve Product Profitability Analysis data: {e}")
    print(f"Failed to retrieve Product Profitability Analysis data: {e}")

# Plotting the data
if not df_product_profitability.empty:
    top_profitable_products = df_product_profitability.head(10)  # Get top 10 products with highest profitability

    plt.figure(figsize=(14, 7))
    sns.barplot(x='Profit', y='ProductName', data=top_profitable_products, palette="viridis")
    plt.title('Top 10 Products by Profitability')
    plt.xlabel('Profit')
    plt.ylabel('Product Name')
    plt.grid(True)
    plt.show()
else:
    logging.error("No data retrieved for product profitability analysis.")

#######################################################

#%% Analyze the cost structure and identify areas for cost reduction
cost_structure_analysis_query = """
SELECT 
    sod.ProductID,
    p.Name AS ProductName,
    SUM(sod.UnitPrice * sod.OrderQty) AS TotalCost,
    SUM(sod.LineTotal) AS TotalSales,
    SUM(sod.LineTotal) - SUM(sod.UnitPrice * sod.OrderQty) AS Profit
FROM 
    Sales.SalesOrderDetail sod
JOIN 
    Production.Product p ON sod.ProductID = p.ProductID
GROUP BY 
    sod.ProductID, p.Name
ORDER BY 
    TotalCost DESC;
"""
try:
    df_cost_structure_analysis = retrieve_data(cost_structure_analysis_query, server, database)
    logging.info("Cost Structure Analysis Data:\n%s", df_cost_structure_analysis.head())
    print("Cost Structure Analysis data retrieved successfully.")
except Exception as e:
    logging.error(f"Failed to retrieve Cost Structure Analysis data: {e}")
    print(f"Failed to retrieve Cost Structure Analysis data: {e}")

#################################################################################

#%% Average Sales and Best-Selling Products per Territory
query_sales_performance = """
SELECT 
    st.Name AS TerritoryName,
    AVG(soh.TotalDue) AS AvgSales,
    p.Name AS BestSellingProduct,
    MAX(soh.TotalDue) AS MaxSale
FROM 
    Sales.SalesOrderHeader soh
INNER JOIN 
    Sales.SalesTerritory st ON soh.TerritoryID = st.TerritoryID
INNER JOIN 
    Sales.SalesOrderDetail sod ON soh.SalesOrderID = sod.SalesOrderID
INNER JOIN 
    Production.Product p ON sod.ProductID = p.ProductID
GROUP BY 
    st.Name, p.Name
ORDER BY 
    AvgSales DESC;
"""
try:
    df_sales_performance = retrieve_data(query_sales_performance, server, database)
    logging.info("Sales Performance Analysis Data:\n%s", df_sales_performance.head())
    print("Sales Performance Analysis data retrieved successfully.")
except Exception as e:
    logging.error(f"Failed to retrieve Sales Performance Analysis data: {e}")
    print(f"Failed to retrieve Sales Performance Analysis data: {e}")

# Plotting the data
if not df_sales_performance.empty:
    plt.figure(figsize=(14, 7))
    ax = sns.barplot(x='AvgSales', y='TerritoryName', data=df_sales_performance, palette="viridis")

    # Annotate the plot with the best-selling product
    for index, row in df_sales_performance.iterrows():
        ax.text(row.AvgSales, index, row.BestSellingProduct, color='black', ha="left", va="center")

    plt.title('Average Sales and Best-Selling Products per Territory')
    plt.xlabel('Average Sales')
    plt.ylabel('Territory Name')
    plt.grid(True)
    plt.show()
else:
    logging.error("No data retrieved for sales performance analysis.")

#########################################################################

#%% Low Inventory Best-Selling Products
query_product_inventory = """
SELECT 
    p.ProductID, 
    p.Name AS ProductName, 
    pi.Quantity AS InventoryQty, 
    COUNT(sod.SalesOrderDetailID) AS SalesCount
FROM 
    Production.Product p
INNER JOIN 
    Production.ProductInventory pi ON p.ProductID = pi.ProductID
INNER JOIN 
    Sales.SalesOrderDetail sod ON p.ProductID = sod.ProductID
GROUP BY 
    p.ProductID, p.Name, pi.Quantity
HAVING 
    pi.Quantity < 10
ORDER BY 
    SalesCount DESC;
"""
try:
    df_product_inventory = retrieve_data(query_product_inventory, server, database)
    logging.info("Low Inventory Best-Selling Products Data:\n%s", df_product_inventory.head())
    print("Low Inventory Best-Selling Products data retrieved successfully.")
except Exception as e:
    logging.error(f"Failed to retrieve Low Inventory Best-Selling Products data: {e}")
    print(f"Failed to retrieve Low Inventory Best-Selling Products data: {e}")

###########################################################

#%% Forecast future sales for Next 12 months
sales_forecasting_query = """
SELECT 
    YEAR(OrderDate) AS Year,
    MONTH(OrderDate) AS Month,
    SUM(TotalDue) AS TotalSales
FROM 
    Sales.SalesOrderHeader
GROUP BY 
    YEAR(OrderDate), MONTH(OrderDate)
ORDER BY 
    Year, Month;
"""
try:
    df_sales_forecasting = retrieve_data(sales_forecasting_query, server, database)
    logging.info("Historical Sales Data for Forecasting:\n%s", df_sales_forecasting.head())
    print("Historical Sales Data for Forecasting retrieved successfully.")
except Exception as e:
    logging.error(f"Failed to retrieve Historical Sales Data for Forecasting: {e}")
    print(f"Failed to retrieve Historical Sales Data for Forecasting: {e}")

df_sales_forecasting['Date'] = pd.to_datetime(df_sales_forecasting[['Year', 'Month']].assign(day=1))
df_sales_forecasting.set_index('Date', inplace=True)

try:
    model = ARIMA(df_sales_forecasting['TotalSales'], order=(1, 1, 1)) 
    model_fit = model.fit()
    forecast = model_fit.forecast(steps=12) 
    logging.info("Forecasted Sales for Next 12 Months:\n%s", forecast)
    print("Forecasted Sales for Next 12 Months generated successfully.")
except Exception as e:
    logging.error(f"Failed to forecast future sales: {e}")
    print(f"Failed to forecast future sales: {e}")

# Plot the actual sales and forecasted sales
plt.figure(figsize=(14, 7))
plt.plot(df_sales_forecasting.index, df_sales_forecasting['TotalSales'], label='Actual Sales')
plt.plot(forecast.index, forecast, label='Forecasted Sales', linestyle='--', color='red')
plt.title('Sales Forecasting for Next 12 Months')
plt.xlabel('Date')
plt.ylabel('Total Sales')
plt.legend()
plt.grid(True)
plt.show()

#%% Assess individual sales performance
individual_sales_performance_query = """
SELECT 
    soh.SalesPersonID,
    p.FirstName,
    p.LastName,
    COUNT(soh.SalesOrderID) AS TotalOrders,
    SUM(soh.TotalDue) AS TotalSales
FROM 
    Sales.SalesOrderHeader soh
JOIN 
    Person.Person p ON soh.SalesPersonID = p.BusinessEntityID
GROUP BY 
    soh.SalesPersonID, p.FirstName, p.LastName
ORDER BY 
    TotalSales DESC;
"""
try:
    df_individual_sales_performance = retrieve_data(individual_sales_performance_query, server, database)
    logging.info("Individual Sales Performance Analysis Data:\n%s", df_individual_sales_performance.head())
    print("Individual Sales Performance Analysis data retrieved successfully.")
except Exception as e:
    logging.error(f"Failed to retrieve Individual Sales Performance Analysis data: {e}")
    print(f"Failed to retrieve Individual Sales Performance Analysis data: {e}")

#%% Productivity metrics using EmployeeDepartmentHistory
productivity_metrics_query = """
SELECT 
    edh.DepartmentID,
    d.Name AS DepartmentName,
    COUNT(e.BusinessEntityID) AS TotalEmployees,
    AVG(e.VacationHours) AS AvgVacationHours,
    AVG(e.SickLeaveHours) AS AvgSickLeaveHours
FROM 
    HumanResources.EmployeeDepartmentHistory edh
JOIN 
    HumanResources.Employee e ON edh.BusinessEntityID = e.BusinessEntityID
JOIN 
    HumanResources.Department d ON edh.DepartmentID = d.DepartmentID
GROUP BY 
    edh.DepartmentID, d.Name
ORDER BY 
    TotalEmployees DESC;
"""
try:
    df_productivity_metrics = retrieve_data(productivity_metrics_query, server, database)
    logging.info("Productivity Metrics Data:\n%s", df_productivity_metrics.head())
    print("Productivity Metrics data retrieved successfully.")
except Exception as e:
    logging.error(f"Failed to retrieve Productivity Metrics data: {e}")
    print(f"Failed to retrieve Productivity Metrics data: {e}")


# Log the completion of the script
logging.info("Script completed.")
print("Script completed.")

######################################################################################
