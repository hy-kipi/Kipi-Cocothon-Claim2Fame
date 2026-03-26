# 🚀 Kipi Cocothon – Claim2Fame

An end-to-end **data application built on Snowflake + Streamlit**, designed to demonstrate modern data engineering, analytics, and AI capabilities using Snowflake’s ecosystem.

---

## 📌 Overview

This project showcases:

- 📊 Data ingestion and transformation pipelines  
- ❄️ Snowflake as the unified data platform  
- 🧠 Snowpark & Cortex for analytics / AI  
- 🎨 Streamlit for interactive UI  
- ⚙️ End-to-end data app architecture  

---

## 🏗️ Architecture


---

## 📁 Repository Structure

### 🔹 `/app`
Contains the main **Streamlit application**

**Files:**
- `app.py` → Entry point of the Streamlit UI  
- UI components, charts, and interaction logic  
- Snowflake session integration  

---

### 🔹 `/data`
Stores:
- Sample datasets  
- Mock data for testing  
- Input files for ingestion  

---

### 🔹 `/sql`
Contains SQL scripts for:

- Table creation (Raw / Staging / Consumption)  
- Transformations  
- Aggregations  
- Business logic  

---

### 🔹 `/snowpark`
Includes Snowpark-based logic:

- Data transformations using Python  
- Business rules implementation  
- DataFrame operations inside Snowflake  

---

### 🔹 `/pipelines`
Defines data movement logic:

- ETL / ELT pipelines  
- Incremental data loading  
- Scheduling logic  

---

### 🔹 `/streamlit_components` *(if present)*
Reusable UI components:

- Charts  
- Filters  
- Custom widgets  

---

### 🔹 `/config`
Configuration files:

- Snowflake connection configs  
- Environment variables  
- Secrets handling  

---

### 🔹 `.streamlit/`
Streamlit configuration:

- `secrets.toml` → Credentials (not committed ideally)  
- UI configs  

---

## ⚙️ Setup Instructions

### 1️⃣ Clone Repository

```bash
git clone https://github.com/hy-kipi/Kipi-Cocothon-Claim2Fame.git
cd Kipi-Cocothon-Claim2Fame

