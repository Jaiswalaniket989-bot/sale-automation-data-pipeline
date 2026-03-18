# 🚀 Sales Automation Data Pipeline

## 📌 Overview
An end-to-end automated data pipeline that processes sales data from CSV files, loads it into PostgreSQL, and visualizes insights in Power BI. The system eliminates manual work and ensures consistent, reliable data processing.

---

## 🎯 Problem
Manual handling of sales data is slow, error-prone, and not scalable. There is a need for an automated system to process, validate, and analyze incoming data efficiently.

---

## 💡 Solution
This project automates the full ETL workflow:
- Ingests multiple CSV files from a data folder  
- Cleans and transforms data (revenue, profit)  
- Removes duplicates using `order_id`  
- Loads data into PostgreSQL  
- Moves processed files to archive folder  
- Updates Power BI dashboard via database connection  

---

## 🏗️ Architecture
CSV Files → Python ETL → PostgreSQL → Power BI Dashboard

---

## ⚙️ Tech Stack
- Python (Pandas, SQLAlchemy)  
- PostgreSQL  
- Power BI  
- Windows Task Scheduler  

---

## 🔄 Automation Workflow
1. Drop CSV file into `data/` folder  
2. Scheduler triggers ETL script  
3. Data is processed and inserted into database  
4. File moves to `processed/` folder  
5. Dashboard reflects updated data  

---

## 📊 Key Features
- Automated ETL pipeline  
- Multi-file ingestion  
- Duplicate handling  
- Revenue & profit calculation  
- Scheduled execution  
- Interactive dashboard  

---

## ▶️ How to Run
'''bash

-pip install pandas sqlalchemy psycopg2-binary

-python etl_pipeline.py

-Update database connection:

-create_engine("postgresql://username:password@localhost:5432/sales")


⚠️ Limitations

-Batch processing only (no real-time)

-No upsert (only inserts new records)

-Depends on scheduler

🔮 Future Improvements

-Upsert logic (update existing records)

-Real-time pipeline (Airflow/Kafka)

-Cloud deployment

📸 Dashboard

<img width="570" height="163" alt="Overview" src="https://github.com/user-attachments/assets/eb1b0ab3-743d-4ec3-b84b-1311d46b99f0" />

<img width="270" height="163" alt="revenue trend" src="https://github.com/user-attachments/assets/166bbe56-4f48-4472-8e4a-140635cb265b" />

<img width="270" height="163" alt="region analysis" src="https://github.com/user-attachments/assets/96e58fa4-8ed5-427c-abc8-279df2e7c367" />
