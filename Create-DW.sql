drop database if exists Metro_Dwh_Project;
create database Metro_Dwh_Project;
use Metro_Dwh_Project;
# creating star schema 
drop table if exists CUSTOMER_DIMENSION;
create table CUSTOMER_DIMENSION(
      CUSTOMER_ID int primary key not null,
	  CUSTOMER_NAME varchar(50) not null,
	  GENDER varchar(10) not null
); 

drop table if exists PRODUCT_DIMENSION;
create table  PRODUCT_DIMENSION(
    PRODUCT_ID int primary key not null,
    PRODUCT_NAME varchar(255) not null,
	PRODUCT_PRICE DECIMAL(10, 2) NOT NULL,
	SUPPLIER_ID int not null,
	SUPPLIER_NAME varchar(255) not null,
	STORE_ID int not null,
	STORE_NAME varchar(255) not null
);

drop table if exists TIME_DIMENSION;
create table TIME_DIMENSION(
    TIME_ID INT   primary key not null,
    Orderdate datetime not null ,
    day INT not null,
    month INT not null,
    quarter INT not null,
    year int not null,
    week int not null,
    day_of_week int not null 
);

drop table if exists Metro_Sales_Fact;
create table Metro_Sales_Fact(
ORDER_ID int primary key not null,
ORDER_DATE datetime not null,
TIME_ID int  not null,
PRODUCT_ID int not null,
CUSTOMER_ID int not null,
QUANTITY int not null,
SALE DECIMAL(15, 2) not null,
foreign key (TIME_ID) references  TIME_DIMENSION(TIME_ID),
foreign key (PRODUCT_ID) references PRODUCT_DIMENSION(PRODUCT_ID),
foreign key (CUSTOMER_ID) references CUSTOMER_DIMENSION(CUSTOMER_ID)
);


select * from PRODUCT_DIMENSION;
select * from CUSTOMER_DIMENSION;
select * from TIME_DIMENSION;
select * from Metro_Sales_Fact;





