# Apache Atlas Governance Layer

## Overview

This module implements the metadata governance layer of the Seismological Data Lakehouse architecture. It is responsible for ensuring data organization, traceability, interoperability, and compliance with FAIR principles (Findable, Accessible, Interoperable, Reusable).

The governance layer is built on top of Apache Atlas and integrates domain-specific metadata models with automated ingestion pipelines.

---

## Architecture Role

Within the Data Lakehouse architecture, this layer operates as a transversal component across all stages:

- Data ingestion
- Storage
- Processing
- Data access

It provides:
- Metadata management
- Data lineage tracking (end-to-end)
- Semantic standardization
- Governance policy enforcement

---

## Directory Structure
- atlas/
- ├── scripts/
- │ ├── ingestao/
- │ ├── eventos/
- │ └── boletim/


---

## Entity Definitions (Typedefs)

The following custom entities are defined to represent domain-specific concepts:

### EstacaoSismologica & DadosDeIngestaoV2
- File: `typedef_ingestao_corrigido.json`
- Represents seismic stations and raw waveform ingestion data (MiniSEED)

### EventoSismico
- File: `typedef_eventos.json`
- Represents detected and processed seismic events

### BoletimSismico
- File: `typedef_boletim_sismico.json`
- Represents curated seismic bulletins and aggregated analytical results

---

## Scripts

### 1. Ingestion (`scripts/ingestao/`)

Responsible for:
- Extracting metadata from MiniSEED files
- Registering seismic stations
- Registering sensor data in Apache Atlas

---

### 2. Events (`scripts/eventos/`)

Responsible for:
- Registering seismic events
- Linking events with ingestion data
- Supporting traceability between raw and processed data

---

### 3. Bulletin (`scripts/boletim/`)

Responsible for:
- Generating seismic bulletins
- Aggregating events into analytical products
- Registering derived datasets in Apache Atlas

---

## Key Features

- End-to-end data lineage tracking
- Integration with HDFS-based storage
- Automated metadata ingestion
- Semantic enrichment using seismological standards (FDSN/IRIS)
- FAIR-compliant data management

---

## Integration with the Architecture

This layer is integrated with:

- Apache Hadoop (distributed storage)
- Apache Spark (data processing)
- Apache Airflow (workflow orchestration)
- JupyterLab (data analysis and visualization)

---

## Reproducibility

All scripts are designed to be reproducible and adaptable to other geoscientific domains, supporting scalable and transparent data workflows.

---

## Scientific Contribution

This module provides a domain-oriented metadata governance model for seismological data, enabling:

- Improved data interoperability
- Enhanced reproducibility
- Transparent and auditable scientific workflows

It represents a practical implementation of FAIR principles within a real-world Data Lakehouse environment.
└── typedefs/

