#!/usr/env/bin python 

import json 
import pandas as pd 
import numpy as np 
import psycopg2
from pprint import pprint
from sqlalchemy import create_engine

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 100)



#Importing  json dataset 

data=[]
filename="/Users/rupehkumarchoudhary/Downloads/immobilienscout24_berlin_20190113.json"
with open(filename ,'r') as f:
    for line in f:
        data.append(json.loads(line))



fact_flat=(
    "id",
    "contactDetails_firstname",
    "contactDetails_lastname",
    "contactDetails_phoneNumber",
    "contactDetails_phoneNumberAreaCode",
    "contactDetails_phoneNumberCountryCode",
    "contactDetails_salutation",
    "contactDetails_email",
    "contactDetails_countryCode",
    "realEstate_apartmentType",
    "contactDetails_address_houseNumber",
    "realEstate_address_city",
    "contactDetails_address_street",
    "realEstate_livingSpace",
    "realEstate_numberOfFloors",
    "realEstate_numberOfBedRooms",
    "realEstate_numberOfRooms",
    "realEstate_numberOfBathRooms",
    "realEstate_balcony",
    "realEstate_calculatedTotalRent",
    "realEstate_heatingCosts",
    "realEstate_serviceCharge"
)

dim_address=(
    "id",
    "contactDetails_address_city",
    "contactDetails_address_houseNumber",
    "contactDetails_address_postcode",
    "contactDetails_address_street"
   
)

dim_agency=(
    "id",
    "contactDetails_firstname",
    "contactDetails_lastname",
    "contactDetails_phoneNumber",
    "contactDetails_email",
    "realEstate_address_city",
    "realEstate_address_postcode"
)


#Result for fact_flat ,dim_address,dim_agency star schema tables

datarow=[]
for i in range(0 ,len(data)):
    data_i=[]
    for j in range(0 ,len(fact_flat)):
        value=data[i]['data'].get(fact_flat[j],None) 
        data_i.append(value)
    datarow.append(data_i)
    
datarow2=[]
for i in range(0 ,len(data)):
    data_i=[]
    for j in range(0 ,len(dim_address)):
        value=data[i]['data'].get(dim_address[j],None) 
        data_i.append(value)
    datarow2.append(data_i)
    
    
datarow3=[]
for i in range(0 ,len(data)):
    data_i=[]
    for j in range(0 ,len(dim_agency)):
        value=data[i]['data'].get(dim_agency[j],None) 
        data_i.append(value)
    datarow3.append(data_i)
    

#Data frame headers 

Headers=(
    "id", 
    "name",
    "lastname",
    "contactnumber",
    "phoneareacode",
    "phonecountrycode",
    "gender",
    "emailid",
    "coutrycode",
"apartmenttype",
    "housenumber",
    "city",
    "street",
    "livingspace",
    "floors",
    "bedrooms",
    "rooms",
    "bathrooms",
    "balcony",
    "totalrent",
    "heatingcosts",
    "servicecharge"
)

Headers1=(
"id",
"city",
"housenumber",
"postcode",
"street"
)

Headers2=(
    "id",
    "firstname",
    "lastname",
    "phonenumber",
    "email",
    "city",
    "postcode"
)


#All three data frames from fact_flat,dim_address and dimm_agency 
df=pd.DataFrame(datarow,columns=Headers,dtype=float)
df1=pd.DataFrame(datarow2,columns=Headers1,dtype=float)
df2=pd.DataFrame(datarow3,columns=Headers2,dtype=float)



#Manuplaing dataframe df 
df.isnull().sum()
df['name'].value_counts(dropna=False)
df.bedrooms.fillna(df.rooms, inplace=True)
df['bathrooms'].fillna(value="1",inplace='True')
df['floors'].fillna(value="5",inplace='True')
df['heatingcosts'].fillna(value="70",inplace='True')
df['servicecharge'].fillna(value="100",inplace='True')

#Manuplating datafrma df1
#Filtering out data where all four rows (city,housenumber,street,postcode) have Nan values 
df1['city'].replace('', np.nan, inplace=True)
df1['housenumber'].replace('', np.nan, inplace=True)
df1['street'].replace('', np.nan, inplace=True)
df1['postcode'].replace('', np.nan, inplace=True)
df1.dropna(subset=['city','housenumber','street','postcode'],how='all',inplace=True)


#Manuplating dataframe df2
df2.firstname.fillna(df.lastname, inplace=True)
df2.dropna(subset=['firstname','lastname','email'],how='any',inplace=True)


#Creating postgressql database class to create and drop tables .

class DatabaseConnection:
    def __init__(self):
        try:
            self.connection=psycopg2.connect("dbname=postgres user=postgres password=postgres")
            self.connection.autocommit=True
            self.cursor=self.connection.cursor()
        except:
            pprint("Could not connect to database")
          
    def create_table(self):
        sql="""CREATE TABLE fact_flat (
            id int primary key,
            name varchar,
            lastname varchar,
            contactnumber varchar,
            phoneareacode varchar,
            phonecountrycode varchar,
            gender varchar,
            emailid varchar,
            coutrycode varchar,
             ApartmentType varchar(40),
             houseNumber varchar(40),
             city varchar(40),
             Street varchar(40),
             livingSpace float,
             Floors int,
             BedRooms int,
             Rooms int,
             BathRooms int,
             Balcony BOOLEAN NOT NULL,
             TotalRent  float(40),
             HeatingCosts float(40),
             ServiceCharge float(40)
        )"""
        sql2="""create Table dim_address( 
            id int REFERENCES fact_flat(id),
            city varchar,
            housenumber varchar,
            postcode varchar,
            street varchar 
        )"""
        sql3="""
              Create Table dim_agency( 
                 "id" int REFERENCES fact_flat(id),
                 "firstname" varchar,
                 "lastname" varchar ,
                 "phonenumber" varchar ,
                 "email" varchar ,
                 "city" varchar ,
                 "postcode" varchar)"""
        try:
            self.cursor.execute(sql)
        except Exception as e:
            pprint("%s" %(str(e)))
        try:    
            self.cursor.execute(sql2)
        except Exception as e:
            pprint("%s" %(str(e)))
        try:
            self.cursor.execute(sql3)
        except Exception as e:
            pprint("%s" %(str(e)))
            
    def drop_table(self):
        drop_sql="""drop table fact_flat cascade"""
        drop_sql1="""drop table dim_address cascade"""
        drop_sql2="""drop table dim_agency cascade"""
        try:
            self.cursor.execute(drop_sql)
        except Exception as e:
            pprint("%s" %(str(e)))
        try:
            self.cursor.execute(drop_sql1)
        except Exception as e:
            pprint("%s" %(str(e)))
        try:
            self.cursor.execute(drop_sql2)
        except Exception as e:
            pprint("%s" %(str(e)))
 

#Creating database object handler follow by drop tables and create tables  
database_connection=DatabaseConnection()
database_connection.drop_table()
database_connection.create_table()


#Creating postgresql database connection object for dataframe to load to postgres 

engine = create_engine('postgresql://postgres:postgres@localhost:5432/postgres')

#Loading data to Postgresql 

df.to_sql('fact_flat', engine,if_exists='append',index=False)
df1.to_sql('dim_address',engine,if_exists='append',index=False)
df2.to_sql('dim_agency',engine,if_exists='append',index=False)





