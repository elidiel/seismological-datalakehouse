# Apache Atlas (Docker)

## Overview

This directory contains the Docker Compose configuration for running **Apache Atlas**, which is responsible for metadata management, data lineage tracking, and governance within the Seismological Data Lakehouse architecture.

Apache Atlas is the only component of the architecture currently containerized, providing an isolated and reproducible environment for data governance.

---

## Purpose

Within the Data Lakehouse architecture, Apache Atlas is used to:

* Register metadata for seismic datasets (MiniSEED, dataless SEED, CSV)
* Track end-to-end data lineage across ingestion, processing, and access layers
* Support FAIR principles (Findability, Accessibility, Interoperability, Reusability)
* Manage relationships between entities such as:

  * Seismological stations
  * Ingested waveform data
  * Seismic events
  * Seismic bulletins

---

## Prerequisites

Before running Apache Atlas via Docker, ensure that the following are installed:

* Docker
* Docker Compose

---

## Usage

Navigate to this directory:


cd governance/docker

Start Apache Atlas:


docker compose up -d


---

## Access

After the container is running, access the Atlas web interface:

id="access-atlas"
http://localhost:21000


### Default Credentials

* **Username:** admin
* **Password:** admin

---

## Configuration Details

The Docker setup uses the official image:

```id="atlas-image"
sburn/apache-atlas:latest
```

### Exposed Ports

* `21000` → Web UI and REST API
* `9026`, `9027` → Internal services

### Data Persistence

Atlas data is persisted locally using:

```id="atlas-volume"
./data → /apache-atlas/data
```

This ensures that metadata and lineage information are preserved across container restarts.

---

## Health Check

The container includes a health check to verify service availability:

```id="atlas-healthcheck"
http://localhost:21000
```

Startup may take a few minutes due to initialization of embedded services (HBase and Solr).

---

## Stop the Service

To stop Apache Atlas:

```bash
docker compose down
```

---

## Notes

* This container uses embedded HBase and Solr instances (`MANAGE_LOCAL_HBASE=true`, `MANAGE_LOCAL_SOLR=true`), suitable for development and research environments.
* For production deployments, external services are recommended.
* Other components of the architecture (Hadoop, Spark, Airflow, JupyterLab) must be installed and managed separately.

---

## Integration in the Architecture

Apache Atlas operates as the **governance layer** of the Seismological Data Lakehouse, integrating with:

* Data ingestion pipelines (Airflow + Python)
* Storage layer (HDFS)
* Processing layer (Spark)
* Analytical and visualization tools (JupyterLab, GMT)

---

## Troubleshooting

### Check running containers

```bash
docker ps
```

### View logs

```bash
docker logs -f apache-atlas
```

### Restart service

```bash
docker compose restart
```

---

## Future Improvements

* Full containerization of the Data Lakehouse stack
* Integration with Kubernetes
* Externalized metadata storage (HBase/Solr clusters)

---
