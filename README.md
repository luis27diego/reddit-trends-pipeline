# 🔥 Reddit Trends Pipeline

[![Python](https://img.shields.io/badge/Python-3.11-blue?logo=python)](https://python.org)
[![Spark](https://img.shields.io/badge/Apache%20Spark-3.5-orange?logo=apachespark)](https://spark.apache.org)
[![Prefect](https://img.shields.io/badge/Prefect-3.x-purple?logo=prefect)](https://prefect.io)
[![Docker](https://img.shields.io/badge/Docker-Compose-blue?logo=docker)](https://docker.com)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-blue?logo=postgresql)](https://postgresql.org)

Pipeline ETL completo para análisis de tendencias del dataset de Reddit sobre Cambio Climático. Procesa millones de comentarios usando **Apache Spark** distribuido, orquestado con **Prefect**, almacenado en **MinIO** y visualizado con **Metabase** y **Power BI**.

---

## 📋 Tabla de Contenidos

- [Características](#-características)
- [Arquitectura](#-arquitectura)
- [Flujo del Pipeline](#-flujo-del-pipeline)
- [Requisitos](#-requisitos)
- [Instalación](#-instalación)
- [Uso](#-uso)
- [Estructura del Proyecto](#-estructura-del-proyecto)
- [Servicios y Puertos](#-servicios-y-puertos)
- [Análisis Disponibles](#-análisis-disponibles)

---

## ✨ Características

- 📥 **Ingesta automatizada** desde Kaggle API
- ⚡ **Procesamiento distribuido** con Apache Spark (Master + 2 Workers)
- 🗄️ **Almacenamiento S3-compatible** con MinIO
- 🔄 **Orquestación de flujos** con Prefect 3.x
- 📊 **Múltiples análisis**: temporal, sentimiento, engagement, texto
- 🎯 **API REST** con FastAPI
- 📈 **Dashboards BI** con Metabase y Power BI
- 🐳 **Containerizado** con Docker Compose

---

## 🏗️ Arquitectura

### Diagrama C4 - Nivel de Contenedores

```mermaid
flowchart TB
    subgraph External["🌐 Fuentes Externas"]
        KG[("📦 Kaggle<br/>Reddit Dataset")]
    end

    subgraph Docker["🐳 Docker Compose"]
        subgraph Orchestration["⚡ Orquestación"]
            PS["Prefect Server<br/>:4200"]
            PW["Prefect Worker"]
        end

        subgraph Spark["🔥 Spark Cluster"]
            SM["Spark Master<br/>:18080"]
            SW1["Worker 1"]
            SW2["Worker 2"]
        end

        subgraph Storage["💾 Storage"]
            MIO[("MinIO<br/>:9000/:9001")]
            PG[("PostgreSQL<br/>:5432")]
        end

        subgraph Presentation["📊 Presentación"]
            API["FastAPI<br/>:8000"]
            MB["PowerBI"]
        end
    end


    KG -->|"1. Descarga"| PW
    PS -->|"Orquesta"| PW
    PW -->|"2. Upload raw/"| MIO
    PW -->|"3. Submit Job"| SM
    SM --> SW1 & SW2
    SW1 & SW2 -->|"4. Lee/Escribe"| MIO
    PW -->|"5. Carga analytics"| PG
    API -->|"Query"| PG
    MB -->|"Query"| PG


    %% === PALETA PASTEL NUEVA ===
    style External fill:#F7C6C7,stroke:#F28B90,color:#5A2A2C,stroke-width:2px
    style Docker fill:#D8C8FF,stroke:#B399FF,color:#3A2A5A,stroke-width:2px
    style Orchestration fill:#FFF3B0,stroke:#FFE066,color:#5A4F1A,stroke-width:2px
    style Spark fill:#FFDAC1,stroke:#FFB899,color:#5A392A,stroke-width:2px
    style Storage fill:#C8F7DC,stroke:#93E9B9,color:#1D3D2B,stroke-width:2px
    style Presentation fill:#C7EFFF,stroke:#9AD7FF,color:#1A3C50,stroke-width:2px
```

---

## 🔄 Flujo del Pipeline

### Pipeline ETL Completo

![diagrama](docs/codigo2.drawio.svg)

### Diagrama de Secuencia

```mermaid
%%{init: {'theme': 'neutral'}}%%
sequenceDiagram
    autonumber
    participant K as Kaggle
    participant PW as Prefect Worker
    participant MI as MinIO
    participant SP as Spark Cluster
    participant PG as PostgreSQL
    participant BI as PoweBI/API

    rect rgb(46, 204, 113, 0.1)
        Note over PW,MI: FLUJO 1: INGESTA
        PW->>K: download_dataset()
        K-->>PW: CSV (raw data)
        PW->>MI: upload_file() → raw/
    end

    rect rgb(241, 196, 15, 0.1)
        Note over PW,SP: FLUJO 2: PROCESAMIENTO
        PW->>PW: run_deployment("procesamiento")
        PW->>SP: create_spark_session()
        SP->>MI: leer_csv_optimizado()
        MI-->>SP: DataFrame
        SP->>SP: Análisis (trends, sentiment, etc.)
        SP->>MI: guardar_resultado() → analytics/
    end

    rect rgb(52, 152, 219, 0.1)
        Note over PW,PG: FLUJO 3: CARGA
        PW->>PW: run_deployment("carga_bd")
        PW->>MI: Lee resultados
        MI-->>PW: CSVs procesados
        PW->>PG: cargar_resultados_a_db()
    end

    rect rgb(231, 76, 60, 0.1)
        Note over PG,BI: CONSULTA
        BI->>PG: SELECT queries
        PG-->>BI: Analytics data
    end
```

---

## 📦 Requisitos

- **Docker** >= 24.0
- **Docker Compose** >= 2.20
- **Kaggle API Key** (para descargar datasets)
- **16GB RAM** mínimo recomendado (Spark + MinIO + Postgres)

---

## 🚀 Instalación

### 1. Clonar el repositorio

```bash
git clone https://github.com/tu-usuario/reddit-trends-pipeline.git
cd reddit-trends-pipeline
```

### 2. Configurar variables de entorno

Crear archivo `.env` en la raíz del proyecto:

```env
# MinIO
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=minioadmin123
MINIO_ENDPOINT=http://minio:9000
MINIO_BUCKET=reddit-data

# PostgreSQL
POSTGRES_USER=prefect
POSTGRES_PASSWORD=prefect123
POSTGRES_DB=prefect_db
POSTGRES_HOST=postgres 

# Spark
SPARK_WORKER_CORES=2
SPARK_WORKER_MEMORY=2g
SPARK_MASTER_URL=spark://spark-master:7077

# Prefect
PREFECT_API_URL=http://prefect-server:4200/api
PREFECT_UI_API_URL=http://localhost:4200/api
```

### 3. Configurar Kaggle API

Colocar tu `kaggle.json` en `~/.kaggle/` o configurar variables de entorno:

```bash
export KAGGLE_USERNAME=tu_usuario
export KAGGLE_KEY=tu_api_key
```

### 4. Levantar los servicios

```bash
docker-compose up -d
```

### 5. Verificar servicios

```bash
docker-compose ps
```

---

## 💻 Uso

### Ejecutar el pipeline completo

```bash
# Acceder al Prefect UI
# http://localhost:4200

# Ejecutar flujo de ingesta (trigger manual o programado)
prefect deployment run "Flujo de Ingesta de Reddit/ingesta-deployment"
```

### Acceder a las interfaces

| Servicio | URL | Descripción |
|----------|-----|-------------|
| Prefect UI | http://localhost:4200 | Orquestador de flujos |
| Spark Master | http://localhost:18080 | Dashboard del cluster |
| MinIO Console | http://localhost:9001 | Administración de storage |
| FastAPI Docs | http://localhost:8000/docs | API REST Swagger |
| Metabase | http://localhost:3000 | Dashboards BI |

---

## 📁 Estructura del Proyecto

```
📦 reddit-trends-pipeline/
│
├── 📂 flows/                          # Flujos de Prefect
│   ├── 📂 ingesta/
│   │   ├── flujo_ingesta.py           # @flow: Descarga desde Kaggle
│   │   └── tasks_ingesta.py           # @task: Upload a MinIO
│   ├── 📂 procesamiento/
│   │   ├── flujo_procesamiento_reddit.py  # @flow: Procesamiento Spark
│   │   └── tasks_procesamiento.py     # @task: Análisis distribuido
│   └── 📂 carga_bd/
│       ├── flujo_carga_bd.py          # @flow: ETL a PostgreSQL
│       └── tasks_carga_bd.py          # @task: Carga de resultados
│
├── 📂 src/
│   ├── 📂 api/                        # FastAPI endpoints
│   ├── 📂 config/                     # Settings y configuración
│   ├── 📂 infrastructure/
│   │   ├── 📂 database/               # Conexión PostgreSQL
│   │   ├── 📂 kaggle/                 # Downloader de datasets
│   │   ├── 📂 spark/                  # Session y operaciones I/O
│   │   └── 📂 storage/                # Cliente MinIO
│   ├── 📂 services/
│   │   ├── 📂 analytics/              # Módulos de análisis
│   │   │   ├── trends.py              # Análisis temporal
│   │   │   ├── sentiment.py           # Análisis sentimiento
│   │   │   ├── engagement.py          # Controversia
│   │   │   ├── anomalies.py           # Detección de picos
│   │   │   ├── text_analysis.py       # Palabras clave
│   │   │   └── reporting.py           # Comparativas
│   │   └── 📂 loader/                 # Carga a BD
│   └── 📂 utils/                      # Utilidades
│
├── 📂 docker/                         # Dockerfiles
│   ├── 📂 api/
│   ├── 📂 base/
│   ├── 📂 spark/
│   └── 📂 worker/
│
├── 📂 sql/                            # Scripts SQL
│   └── 📂 init/                       # Inicialización BD
│
├── docker-compose.yaml                # Orquestación de servicios
├── prefect.yaml                       # Configuración Prefect
├── tablero.pbix                       # Dashboard Power BI
└── README.md
```

---

## 🌐 Servicios y Puertos

| Servicio | Puerto | Descripción |
|----------|--------|-------------|
| **MinIO API** | 9000 | API S3-compatible |
| **MinIO Console** | 9001 | Interfaz web de administración |
| **Spark Master UI** | 18080 | Dashboard del cluster Spark |
| **Spark Worker 1** | 18081 | UI Worker 1 |
| **Spark Worker 2** | 18082 | UI Worker 2 |
| **PostgreSQL** | 5432 | Base de datos |
| **Prefect UI** | 4200 | Orquestador de flujos |
| **FastAPI** | 8000 | REST API |
| **Metabase** | 3000 | BI Dashboards |

---

## 📊 Análisis Disponibles

El pipeline genera los siguientes análisis sobre los comentarios de Reddit:

### 🕐 Análisis Temporal
- **Tendencias diarias**: Volumen de comentarios por día
- **Patrones horarios**: Actividad por hora del día
- **Detección de anomalías**: Picos inusuales de actividad

### 💬 Análisis de Sentimiento
- **Distribución sentiment vs score**: Correlación entre sentimiento y puntuación
- **Comentarios extremos**: Los más positivos y negativos

### 📈 Análisis de Engagement
- **Controversia por subreddit**: Métricas de engagement por comunidad

### 📝 Análisis de Texto
- **Palabras clave por sentimiento**: Top palabras asociadas a cada sentimiento

### 📋 Reportes Consolidados
- **Comparativa de subreddits**: Métricas comparativas entre comunidades

---

## 🛠️ Tecnologías

| Categoría | Tecnología | Versión |
|-----------|------------|---------|
| Lenguaje | Python | 3.11 |
| Procesamiento | Apache Spark | 3.5 |
| Orquestación | Prefect | 3.x |
| Storage | MinIO | Latest |
| Base de Datos | PostgreSQL | 15 |
| API | FastAPI | Latest |
| BI | Metabase | Latest |
| BI Desktop | Power BI | - |
| Contenedores | Docker Compose | - |

---

## 📄 Licencia

Este proyecto está bajo la licencia MIT. Ver el archivo `LICENSE` para más detalles.

---

## 👥 Contribuir

Las contribuciones son bienvenidas. Por favor, abre un issue primero para discutir los cambios que te gustaría hacer.

---

<p align="center">
  Hecho con ❤️ usando Apache Spark, Prefect y Docker
</p>