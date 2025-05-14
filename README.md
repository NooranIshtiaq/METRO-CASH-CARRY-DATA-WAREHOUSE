🏬 METRO Cash & Carry — Data Warehouse Prototype
📋 Prerequisites
💻 System Requirements
MySQL Server 8.0 or above

DBeaver (Database management tool)

Eclipse IDE for Java Developers

📦 Required Libraries / Dependencies
MySQL Connector/J (8.4) – Ensure the .jar file is added to your Java project's build path.

📁 Dataset Files
Ensure the following datasets are available:

customers_data.csv – Contains customer records

products_data.csv – Contains product details

transactions.csv – Contains transactional data

🛠️ Step-by-Step Setup Instructions
🔸 1. Set Up the Database
Open DBeaver

Create a database:

sql
Copy
Edit
CREATE DATABASE Metro_Dwh_Project;
Execute the provided SQL script to create the required tables:

CUSTOMER_DIMENSION

PRODUCT_DIMENSION

TIME_DIMENSION

METRO_SALES_FACT

Load your CSV datasets (customers_data.csv, products_data.csv, and transactions.csv) into the corresponding tables.

🔸 2. Configure Java Project
Open Eclipse (or your preferred Java IDE)

Create a new Java project

Add MySQL Connector/J (8.4) to your project’s referenced libraries:

Right-click project → Build Path → Add External Archives → Select .jar file

Configure database credentials inside your Main.java file:

java
Copy
Edit
String url = "jdbc:mysql://localhost:3306/Metro_Dwh_Project";
String user = "your_username";
String password = "your_password";
Create a package named:
 "mesh_Join "
and place your Java file(s) inside this package.
You’re now ready to run your METRO Data Warehouse Prototype and explore powerful analytics from your dimensionally-modeled warehouse
