# 🧬 Wearable Biomarkers for Prediabetes Detection | PostgreSQL Project

## 🔗 Data Source  
[PhysioNet: BIG IDEAs Lab Glycemic Variability and Wearable Device Data (v1.1.2)](https://physionet.org/content/big-ideas-glycemic-wearable/1.1.2/)

---

## 📁 Dataset Overview

This study evaluated the feasibility of using wearable devices to detect early physiological changes prior to the onset of prediabetes. It generated digital biomarkers for remote, mHealth-based risk classification to identify individuals who may benefit from further clinical testing.

### 👩‍⚕️ Inclusion Criteria
- Age: 35–65 years  
- Gender: Post-menopausal females  
- A1C range: 5.2–6.4% (point-of-care measurement)

### 🧪 Clinical & Wearable Data
- **Blood tests:** Glucose, A1C, lipoproteins, triglycerides  
- **Devices:**  
  - Dexcom G6 CGM (Interstitial glucose every 5 min)  
  - Empatica E4 wristband (HR, IBI, EDA, TEMP, ACC)  
- **Protocol:** 10-day monitoring with standardized breakfast every other day  
- **Final Visit:** Oral Glucose Tolerance Test (OGTT)

---

## 🗃️ Database Design (PostgreSQL)

### 🧱 Schema Architecture: Star Schema

- **Fact Table:** `demography`  
- **Dimension Tables:**  
  - `dexcom` (glucose readings)  
  - `foodlog` (meal intake)  
  - `ibi` (interbeat interval)  
  - `hr` (heart rate)  
  - `temp` (skin temperature)  
  - `eda` (electrodermal activity)

> **Cardinality:** Many-to-one — each patient has multiple physiological and glucose records

---

## 🧼 Data Transformation & Engineering

- Manual data cleaning and timestamp alignment  
- Derived features:  
  - Fasting glucose  
  - Pre- and post-meal windows  
  - Hourly and daily biomarker summaries  
- Created summary tables using **materialized views**  
- Built alert system using **triggers** and **stored procedures**

---

## ⚙️ Performance Optimization & Reusability

To ensure scalable and efficient querying across large physiological datasets, the following advanced PostgreSQL techniques were applied:

- **Indexes:**  
  - Composite indexes on `patient_id`, `timestamp`  
  - Dramatically improved query speed for time-series joins and alert triggers

- **Common Table Expressions (CTEs):**  
  - Used for modular, readable logic in multi-step transformations  
  - Enabled reusable glucose window calculations and biomarker aggregations

- **Range Partitioning:**  
  - Applied to foodlog table as it has two different year data's for patient
  - Reduced scan time and improved performance for hourly/daily summaries

- **Stored Procedures & Triggers:**  
  - Automated alert generation for abnormal glucose or physiological spikes or high risk Foodlog entry  
  - Enabled real-time updates to summary tables and patient flags

- **Materialized Views:**  
  - Created for consolidated patient-level summaries  
  - Refreshed periodically to balance performance and freshness

---

## 🧠 Analytical Highlights

### 🔍 Physiological Insights
- Hourly glucose trends and variability  
- Daily biomarker fluctuations across HR, TEMP, EDA, IBI  
- Meal-linked glucose spikes and recovery windows

### ⚠️ Alert System
- Trigger-based alerts for abnormal glucose or physiological patterns  
- Stored procedures for automated patient-level summaries

---


