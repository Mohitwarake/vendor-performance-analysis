import sqlite3
import pandas as pd
import logging
from ingestion_db import ingest_db

with open("logs/test.txt", "w") as f:
    f.write("test")

logging.basicConfig(
    filename = "logs/Get_vendor_summery.log",
    level = logging.DEBUG,
    format = "%(asctime)s - %(levelname)s - %(message)s",
    filemode = 'a'
)

def create_vendor_summery(conn):
    vendor_sales_summery = pd.read_sql_query(""" WITH frightsummery as(
    SELECT VendorNumber,
    sum(Freight) as freightcost
    from vendor_invoice
    group by VendorNumber
    ),
    
    purchasesummery as(
        SELECT 
            p.VendorNumber,
            p.VendorName,
            p.Brand,
            p.Description,
            p.PurchasePrice,
            pp.Price as actual_price,
            pp.Volume,
            sum(p.Quantity) as total_purchase_qty,
            sum(p.Dollars) as total_purchase_dollars
        from purchases p join purchase_prices pp 
        on p.Brand = pp.Brand
        where p.PurchasePrice > 0
        group by p.VendorNumber, p.VendorName , p.Brand, p.Description, p.PurchasePrice, pp.Price , pp.Volume
    ),
    
    salessummery as(
        SELECT
            VendorNo,
            Brand,
            sum(SalesQuantity) as Total_sales_qty,
            sum(SalesDollars) as Total_sales_Dollers,
            sum(SalesPrice) as Total_sales_price,
            sum(Excisetax) as Total_sales_excisetax
        from sales
        group by VendorNo, Brand
    )
    
    SELECT ps.VendorNumber,
           ps.VendorName,
           ps.Brand,
           ps.Description,
           ps.PurchasePrice,
           ps.actual_price,
           ps.volume,
           ps.total_purchase_qty,
           ps.total_purchase_dollars,
           ss.Total_sales_qty,
           ss.Total_sales_price,
           ss.Total_sales_Dollers,
           ss.Total_sales_excisetax,
           f.freightcost
    FROM purchasesummery ps
    LEFT JOIN salessummery ss ON ps.VendorNumber = ss.VendorNo AND ps.Brand = ss.Brand
    LEFT JOIN frightsummery f ON ps.VendorNumber = f.VendorNumber
     order by total_purchase_dollars""" ,conn)
    return vendor_sales_summery

def clean_data(df):
    df["Volume"] = df["Volume"].astype(float)
    df.fillna(0 , inplace=True)
    df["VendorName"] = df["VendorName"].str.strip()
    df["Description"] = df["Description"].str.strip()
    vendor_sales_summery["GrossProfit"] = vendor_sales_summery["Total_sales_Dollers"] - vender_sales_summery["total_purchase_dollars"]  
    vendor_sales_summery["ProfitMargin"]=(vendor_sales_summery["GrossProfit"] / vendor_sales_summery["Total_sales_Dollers"])*100
    vendor_sales_summery["StockTurnover"] =( vendor_sales_summery["Total_sales_qty"] / vendor_sales_summery["total_purchase_qty"])
    vendor_sales_summery["Salestopurchaseratio"]=(vendor_sales_summery["Total_sales_Dollers"] / vendor_sales_summery["total_purchase_dollars"])
    return df

if ___name___ == "___main___":
    conn=sqlite3.connect('inventory.db')

    logging.info('Creating Vendor summery table...................')
    summery_df = create_vendor_summery(conn)
    logging.info(summery_df.head())

    logging.info('Cleaning Data.................')
    clean_df = clean_data(summery_df)
    logging.info(clean_data.head())

    logging.info('Ingesting Data......................')
    ingest_db(clean_df , 'vendor_sales_summery', conn)
    logging.info('Completed')