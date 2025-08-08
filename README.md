# 🎧 Spotify Lakehouse Integration

A modern data platform project built on Medallion Architecture (Bronze, Silver, Gold) to process and analyze Spotify streaming data. This project demonstrates end-to-end data engineering capabilities—from ingestion to analytics—using Apache Spark, Delta Lake, and Azure technologies.

---

## 🧠 Project Overview

This solution integrates Spotify streaming data and builds a scalable lakehouse architecture to deliver curated insights on:

- Artist popularity metrics  
- User listening patterns  
- Genre trends and dynamics  

It emphasizes clean, reliable data pipelines with data quality validation and metadata-driven orchestration.

---

## Storytelling
Medium blog: https://karetech.medium.com/the-medallion-architecture-spoti-py-case-study-the-lake-house-model-the-beginning-822db0372feb

---
## 🛠️ Tech Stack

| Layer           | Technology                          | Description                                                                 |
|----------------|--------------------------------------|-----------------------------------------------------------------------------|
| Storage         | Azure Data Lake Storage (ADLS)       | Multi-zone storage for raw to curated datasets                             |
| Processing      | Apache Spark + PySpark               | Distributed data transformation and analysis                               |
| Orchestration   | Synapse Pipelines / Azure Functions  | Scheduled movement of data across architecture layers                      |
| Tables          | Delta Lake                           | Versioned, ACID-compliant tables across all layers                         |
| Security        | Azure Key Vault / Entra ID           | Secures access to secrets and credentials                                  |
| Metadata        | Metadata DB + Parameterization       | Dynamic runtime configuration across notebooks                             |
| Visualization   | Databricks Dashboards / Power BI     | Interactive dashboards for final Gold Layer outputs                        |

---

## 🧱 Architecture Design

### Medallion Structure

- **🔹 Bronze Layer** – Raw ingestion from Spotify API (JSON files via Azure Function or notebooks).
- **⚙️ Silver Layer** – Cleaned and enriched data with deduplication, standardization, and joins.
- **🏆 Gold Layer** – Curated, business-ready metrics and analytics for reporting and dashboards.

### Data Zones in ADLS

- `landing/databricks/music/spotify/web-api/entityname`
- `lake/bronze/music/spotify/web-api/entityname`
- `lake/silver/music/spotify/web-api/entityname`
- `lake/gold/music`


### Schema Names in Databricks
- spotify_raw
- spotify_cleansed
- spotify_modelled




---

## 📊 Key Features

- Developed modular PySpark notebooks for batch transformations.
- Automated data ingestion using Azure Logic Apps and Functions.
- Designed Spark SQL stored procedures for listening to trends and artist KPIs.
- Implemented dynamic, metadata-driven pipelines for scalable operations.
- Enabled future extensibility to other platforms (e.g., Apple Music, SoundCloud).
- Designed with source-system alignment and transparency in mind for traceability.

---

## 📈 Sample Use Cases

- Top 10 most-streamed artists per region  
- Weekly listening trends by genre  
- Monthly active users per playlist cluster  

---

## 🔐 Security & Compliance

- Secrets stored in Azure Key Vault  
- Enforced data integrity through schema validation  
- Includes source system metadata for audit traceability  

---

## 🚀 Future Enhancements

- Integrate Apple Music & SoundCloud APIs  
- Add CI/CD using GitHub Actions + Databricks CLI  
- Implement row-level security on Gold datasets  
- Expand to real-time ingestion via Event Hubs or Kafka  

---

<pre>
## 📁 Project Structure
<code>
spotify-lakehouse/
├── ingestion/           # PySpark notebooks for ingesting raw Spotify data (Bronze Layer)
├── transformation/      # Data cleaning, enrichment & joins for Silver Layer processing
├── aggregations/        # Gold Layer transformations: metrics, KPIs, and aggregations
├── pipelines/           # Synapse/ADF pipeline templates for orchestration
├── utils/               # Metadata-driven configs, reusable functions, and schema templates
├── docs/                # Architecture diagrams, ERDs, and project documentation
└── README.md            # Main project readme with overview and usage instructions
</code>
</pre>


---

## 🧑‍💻 Author

**Patrick Okare.**  
Certified Azure Data Engineer | Enterprise Data Platform Engineer  
[LinkedIn Profile] https://www.linkedin.com/in/patrickokare/
---
  - Lead Data Engineer | Data Storyteller | Building Lakehouses with Purpose
  
## ⭐ Acknowledgements

- Spotify API  
- Databricks Lakehouse Guides  
- Microsoft Azure Data Engineering community

---

## 📝 License

This project is licensed under the MIT License.

