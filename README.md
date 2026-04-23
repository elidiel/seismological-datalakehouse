# Seismological Data Lakehouse Architecture

## Overview

This repository provides the implementation of a FAIR-compliant Data Lakehouse architecture designed for seismological data management. The architecture supports the integration, processing, governance, and dissemination of heterogeneous seismic datasets within a unified and scalable framework.

The proposed approach is part of a research study focused on improving reproducibility, traceability, and interoperability in seismological workflows, particularly in the context of regional monitoring systems in Northeastern Brazil.

---

## Architecture Description

The architecture follows a layered Data Lakehouse paradigm composed of four main layers:

### Ingestion Layer

Automated data acquisition using Apache Airflow and Python pipelines, integrated with ObsPy for MiniSEED processing.

### Storage Layer

Distributed storage using Hadoop HDFS, enabling scalable and fault-tolerant data persistence.

### Processing Layer

Data transformation and analysis using Apache Spark and domain-specific tools such as SeisComP3.

### Access Layer

Interactive data access through JupyterLab, APIs, and visualization tools.

### Governance Layer (Transversal)

Implemented using Apache Atlas, ensuring:

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
├── ingestion/        # Airflow DAGs and ingestion pipelines
├── processing/       # Spark jobs and data transformation scripts
├── governance/       # Apache Atlas scripts, typedefs, and Docker setup
│   ├── atlas_scripts/
│   ├── atlas_typedefs/
│   └── docker/
│       └── docker-compose.yml
├── notebooks/        # Jupyter notebooks for analysis and visualization
├── data/             # Sample datasets (if available)
├── docs/             # Architecture diagrams and documentation
└── README.md
```

---

## Data Sources

The architecture is designed to process real-world seismological data, including:

* Waveform data in MiniSEED format (HHE, HHN, HHZ)
* Station metadata in dataless SEED format
* Seismic event data from SeisComP3 databases

Due to data volume and access restrictions, raw seismic data (MiniSEED and dataless SEED formats) are not publicly distributed in this repository.

These datasets are large-scale and require authorization from the Laboratory of Seismological Studies (LABSIS) for access and use.

To support reproducibility, synthetic or sample datasets may be provided, along with scripts and workflows that replicate the data processing and governance pipeline.

---

## Prerequisites

This project relies on a distributed data ecosystem. The following tools must be installed and configured prior to execution:

* Apache Hadoop (HDFS + YARN)
* Apache Spark
* Apache Airflow
* Apache Atlas
* Python 3.10+
* JupyterLab
* GMT (Generic Mapping Tools)

> ⚠️ Note: This repository does not install these tools automatically. A properly configured environment is required.

---

## Service Initialization

After installing the required tools, start the services as follows:

### Hadoop

```
start-dfs.sh
start-yarn.sh
```

### Apache Atlas (Docker)

```
cd governance/docker
docker compose up -d
```

Access: http://localhost:21000

### Apache Airflow

```
conda activate airflow310
airflow webserver -p 8080
airflow scheduler
```

Access: http://localhost:8080

### JupyterLab

```
jupyter lab
```

Access: http://localhost:8888/lab

---

## Pipeline Execution

The typical workflow of the architecture is:

1. Execute Airflow DAGs to ingest raw seismic data into HDFS
2. Process data using Spark and Python-based pipelines
3. Register metadata and lineage in Apache Atlas
4. Generate seismic bulletins and derived artifacts (CSV, PDF, HTML maps)
5. Analyze and visualize results using JupyterLab

---

## Docker Support

Currently, only Apache Atlas is containerized using Docker Compose.

To start Atlas:

```
cd governance/docker
docker compose up -d
```

Other components (Hadoop, Spark, Airflow) must be installed and managed manually.

---

## Reproducibility

This repository includes:

* Scripts for data ingestion and processing
* Metadata and lineage registration workflows
* Example pipelines for seismic data handling

### Reproducibility Note

Due to the complexity of the environment and restrictions on real seismic datasets, full reproducibility requires:

* Access to authorized seismic data sources (e.g., LABSIS)
* Proper configuration of distributed infrastructure (Hadoop, Spark, Airflow, Atlas)

This repository focuses on providing the architecture, workflows, and governance mechanisms necessary to reproduce the methodology, even when full datasets are not publicly available.

---

## Installation (Simplified)

```
git clone https://github.com/elidiel/seismological-datalakehouse.git
cd seismological-datalakehouse
pip install -r requirements.txt
```

---

## Scientific Contribution

This repository supports a Data Lakehouse architecture that:

* Bridges the gap between data engineering and seismological workflows
* Enables traceable and reproducible scientific pipelines
* Integrates FAIR principles directly into system design
* Advances data governance in geoscientific infrastructures

---

## Citation

If you use this repository in your research, please cite:

```
(INSERIR AQUI O BIBTEX DO SEU PAPER)
```

---

## License

This project is licensed under the MIT License.

---

## Contact

Elidiel Dantas da Costa
Federal University of Rio Grande do Norte (UFRN)

For questions or collaboration, feel free to reach out.
