# METRO CASH AND DATA WAREHOUSE PROTOTYPE
Prerequisites
1. System Requirements
a. MySQL Server 8.0 or above.
b. DBEAVER 
c. Eclipse IDE for Java Developers 
2. Libraries/Dependencies
a. MySQL Connector/J(8.4) (.jar file)
3. Dataset Requirements
a. customers_data.csv: Contains customer data.
b. products_data.csv: Contains product data.
c. transactions.csv: Contains  transaction data.
  
Step-by-Step Instructions
1. Set Up the Database
2. Open DBEAVER 
3. Create a database named  Metro_Dwh_Project:
4. Execute the provided SQL script to create the necessary tables:
 - CUSTOMER_DIMENSION
 - PRODUCT_DIMENSION
 - TIME_DIMENSION
 - METRO_SALES_FACT
5. load customers_data.csv, products_data.csv, and transactions.csv.
configure Java Project
   - Open Eclipse or your Java IDE.
   - Create a new project 
   - Set Up Dependencies
   - Add the MySQL Connector(8.4) to your referenced library of project's build path.
   - Configure Database Credentials:
   - Open the Main.java file.
   - Update variables with your MySQL credentials: 
   - make package class named mesh-Join and place the java file inside the class .
