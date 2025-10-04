# ETL de Conciliación Operativa (Demo Sintética)

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Pipeline ETL robusto para conciliación de archivos operativos/contables con observabilidad completa.

## Características

- ✅ Ingesta automática de CSVs con validaciones de esquema
- ✅ Control de totales y checksums
- ✅ Deduplicación idempotente basada en hash
- ✅ Carga transaccional a SQL Server/PostgreSQL
- ✅ Logs estructurados y métricas de performance
- ✅ Reportes automáticos (Excel/PDF)
- ✅ Orquestación con Prefect/Airflow

## Como Replicar Proyecto
```bash
# Clonar repositorio
git clone https://github.com/marcorios99/prj-peru-dataops-etl.git
cd prj-peru-dataops-etl

# Crear entorno virtual
python -m venv venv
source venv/bin/activate  # Linux/Mac
# venv\Scripts\activate   # Windows

# Instalar dependencias
pip install -r requirements.txt

# Generar datos sintéticos
python scripts/generate_sample_data.py --rows 10000

# Ejecutar pipeline
python scripts/run_etl.py --file data/input/operaciones_2025.csv
```

# Arquitectura
```bash
CSV Input → Validación → Deduplicación → Carga SQL → Reporte
            (Pandera)    (Hash-based)    (UPSERT)    (Excel/PDF)
```
Métricas de Ejemplo

- Procesamiento: ~5,000 registros/segundo
- Detección de duplicados: 100% precisión
- Tiempo promedio: 45 segundos para 10k registros
- Tasa de error: < 0.1%

### Proyecto de demostración para portfolio.