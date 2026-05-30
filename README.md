# 🚕 NYC Taxi Analytics Pipeline & Dashboard

## 📌 Project Overview
This project provides an end-to-end data analytics pipeline and business intelligence solution for New York City taxi trip records, processing large-scale spatial and temporal ride data. By utilizing a hybrid engineering architecture—**Pandas** for intensive exploratory programming and **PySpark** for highly scalable database writes—the system cleanses raw GPS telemetry, uncovers hidden urban mobility bottlenecks, and structures the data for real-time reporting. 

The project culminates in an enterprise-grade interactive dashboard deployed via Databricks Lakeview, transforming raw trip files into actionable spatial and operational insights.

---

## 🎯 Key Objectives & Business Questions
* **Macro KPIs & Volume:** What is the total operational throughput of trips, average trip duration, and total passenger capacity across the city?
* **Time-Series Traffic Trends:** How do transit volumes and congestion metrics fluctuate hourly, daily, and across structural transitions like weekdays vs. weekends?
* **Geospatial Hot Spots:** Where are the highest-density pickup and dropoff cluster locations, and where do traffic flows choke?
* **Anomalous Behavior Tracking:** Can we systematically isolate outlier behaviors—specifically, identifying the longest and shortest trip paths—to optimize dispatch efficiency?

---

## 🛠️ Tools & Techniques
* **Language:** Python 3, PySpark Core & SQL APIs
* **Environment:** Databricks Community Edition (Lakehouse Platform)
* **Libraries:** Pandas, NumPy, Plotly Express
* **Techniques Demonstrated:**
  * **Big Data Cleaning & Schema Integrity:** Implementing coordinate filtering grids (Latitude -90 to 90; Longitude -180 to 180) and removing logical outliers like negative durations or zero passenger rides.
  * **Feature Engineering:** Programmatically extracting datetime components (Hours, Days of Week, Weekday/Weekend classification flags) from raw timestamp records.
  * **Lakehouse Table Ingestion:** Transforming processed Pandas structures into PySpark DataFrames to perform production-ready table overwrites (`overwriteSchema=True`) into the **Databricks Unity Catalog**.
  * **Interactive Visualization & Jittering:** Using advanced Plotly visualizations, including custom text overlays, marker styling, and manual uniform data-jittering to reveal patterns in dense categorical fields.

---

## 💡 Key Insights Discovered
* **Temporal Demand Curves:** Hourly trip distributions expose predictable traffic surges during morning commuter rushes and evening social hours, matching contrasting weekday and weekend patterns.
* **Geospatial Concentration:** Specific localized transportation hubs dominate top-line transit totals. The repository explicitly isolates these clusters into specialized data views like `nyc_most_trips_location`.
* **Passenger Distribution Profiles:** Clear limits appear when plotting Passenger Count against Trip Duration, allowing operators to distinguish standard individual commutes from large-group multi-passenger trips.

---

## 📂 Repository Structure

* [NYC Taxi Analytics.ipynb](NYC Taxi Analytics.ipynb): The core engineering notebook containing the full source code for data ingestion, cleaning, transformation logic, and Plotly visualizations.
* [NYC Taxi Analytics.py](NYC Taxi Analytics.py): The refactored production script version structured for modular deployment and automated scheduling workflows within Databricks.
* [NYC Taxi Analytics HTML.html (Interactive Preview)](NYC Taxi Analytics%20HTML.html): A fully rendered HTML asset that preserves and displays all interactive Plotly plots directly in your web browser.
* [NYC Taxi Analytics Dashboard.lvdash (2).json](NYC Taxi Analytics%20Dashboard.lvdash%20(2).json): The native Databricks Lakeview Dashboard blueprint configuration detailing query links, global filters, and UI configurations.
* [NYC Taxi Analytics Dashboard HTML.pdf](NYC Taxi Analytics%20Dashboard%20HTML.pdf): An executive visual summary showing the final layout, heatmaps, and performance charts of the analytics dashboard.

---

## 💾 Data Source
The data utilized in this analytics pipeline is derived from the official NYC Taxi dataset, capturing structural urban trip attributes:
* **Dataset Source:** [Kaggle - NYC Taxi Trip Duration](https://www.kaggle.com/datasets/yasserh/nyc-taxi-trip-duration)

---

## ⚠️ Platform Deployment & Publishing Note
This infrastructure was intentionally built using the **Databricks Community Edition (Free Version)** to demonstrate cost-efficient, enterprise-grade big data engineering. 

Because Databricks sandbox environments restrict native public dashboard hosting and URL generation on the free tier, the live dashboard cannot be rendered via an active web link. To bypass this platform constraint and ensure full visibility, the entire operational interface layout and interactive plots are preserved and viewable through the [Dashboard PDF](NYC Taxi Analytics%20Dashboard%20HTML.pdf) and [Notebook HTML](NYC Taxi Analytics%20HTML.html) components included above.
