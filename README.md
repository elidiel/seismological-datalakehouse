# Seismological Data Lakehouse Architecture

## Overview

This repository provides the implementation of a FAIR-compliant **Seismological Data Lakehouse architecture** designed for the integration, processing, governance, and dissemination of heterogeneous seismic data.

The architecture was developed as part of a research study focused on improving **reproducibility, traceability, and interoperability** in seismological workflows, particularly in the context of **regional seismic monitoring systems in Northeastern Brazil**.

---

## Architecture

The proposed architecture follows a layered Data Lakehouse paradigm composed of four main layers:

* **Ingestion Layer**
  Automated data acquisition using Apache Airflow and Python pipelines integrated with ObsPy for MiniSEED processing.

* **Storage Layer**
  Distributed storage using Hadoop HDFS, ensuring scalable and fault-tolerant data persistence.

* **Processing Layer**
  Data transformation and analysis using Apache Spark and domain-specific tools such as SeisComP3.

* **Access Layer**
  Interactive data access through JupyterLab, APIs, and visualization tools.

A transversal **Governance Layer**, implemented with Apache Atlas, provides:

* Metadata standardization
* Data lineage tracking
* FAIR compliance (Findability, Accessibility, Interoperability, Reusability)

---

## Key Features

* End-to-end data lineage from raw seismic data to derived products
* Integration of heterogeneous data sources (MiniSEED, dataless SEED, CSV)
* Automated ingestion and processing pipelines
* FAIR-aligned metadata management
* Support for seismic event cataloging and bulletin generation

---

## Repository Structure

```
.
├── ingestion/        # Airflow DAGs and ingestion scripts
├── processing/       # Spark jobs and transformation pipelines
├── governance/       # Apache Atlas metadata and lineage scripts
├── notebooks/        # Jupyter notebooks for analysis and visualization
├── data/             # Sample datasets (if available)
├── docs/             # Architecture diagrams
└── README.md
```

---

## 🚀 Quick Start

### 1. Clone the repository

```bash
git clone https://github.com/elidiel/seismological-datalakehouse.git
cd seismological-datalakehouse
```

---

### 2. Start services (Docker)

```bash
docker compose up -d
```

---

### 3. Access services

* Apache Atlas → http://localhost:21000
* Airflow → http://localhost:8080
* JupyterLab → http://localhost:8888

---

### 4. Run ingestion pipeline

```bash
airflow dags trigger dag_ingestao_brutos
```

---

### 5. Verify data in HDFS

```bash
hadoop fs -ls /sismologia/bruta
```

---

## Data Sources

The architecture supports real-world seismological data, including:

* Waveform data in MiniSEED format (HHE, HHN, HHZ)
* Station metadata in dataless SEED format
* Seismic event data from SeisComP3 databases

Due to data access restrictions, only sample or synthetic datasets may be included.

---

## Reproducibility

This repository includes:

* End-to-end data pipelines (ingestion → processing → governance → access)
* Scripts for metadata registration in Apache Atlas
* Containerized services using Docker Compose

Some components (e.g., full Hadoop cluster and SeisComP3 integration) may require manual setup.

---

## Scientific Contribution

This work contributes by:

* Bridging data engineering and seismological workflows
* Enabling reproducible and traceable scientific pipelines
* Embedding FAIR principles into a Data Lakehouse architecture

---

## Citation

If you use this repository, please cite:

```
(Adicionar aqui o BibTeX do seu artigo após aceitação)
```

---

## License

MIT License

---

## Contact

**Elidiel Dantas da Costa**
Federal University of Rio Grande do Norte (UFRN)
