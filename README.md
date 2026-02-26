# Weather Lakehouse (Databricks) — Bronze → Silver → Gold + Data Quality

Pipeline **Lakehouse** para transformar dados meteorológicos em camadas **Silver/Gold** no **Databricks**, com **deduplicação por execução (`run_ts`)** e **relatório de Data Quality**.

> ✅ **Ingestão (Azure Functions) ficou em outro repositório:**
> https://github.com/GabrielContesini/weather-lakehouse

---

## 🎯 Objetivo

Demonstrar prática real de Engenharia de Dados com:

- arquitetura **Bronze/Silver/Gold**
- particionamento por data (`dt=YYYY-MM-DD`)
- dedupe por cidade pegando o `run_ts` mais recente
- outputs em **Parquet**
- checks automatizados de **Data Quality** + `dq_report.json`

---

## 🧱 Arquitetura

**Fonte** → **Bronze (JSON)** → **Silver (Parquet Hourly)** → **Gold (Parquet Daily)** → **DQ Report (JSON)**

- **Fonte**: Open-Meteo (Historical/Archive API)
- **Bronze**: JSON bruto (gerado pelo projeto de Azure Functions)
- **Databricks**:
  - leitura dos JSONs do Bronze
  - dedupe por cidade (`city_id`)
  - flatten da estrutura hourly → **Silver**
  - agregações diárias → **Gold**
  - validações e relatório → **DQ**

---

## 📦 Camadas e estrutura

### Bronze (entrada no Databricks)

Você já tem os arquivos aqui:

/Volumes/weather/weather/raw/bronze/openmeteo/dt=YYYY-MM-DD/\*.json

### Saídas (curated)

/Volumes/weather/weather/curated/silver/weather_hourly/dt=YYYY-MM-DD/.parquet
/Volumes/weather/weather/curated/gold/weather_daily/dt=YYYY-MM-DD/.parquet
/Volumes/weather/weather/curated/quality/reports/dt=YYYY-MM-DD/dq_report.json

---

## 📓 Notebooks (portfólio)

Sugestão de nomes bem “portfólio” (ordem + ação + camada):

1. `01_bronze_ingestion_validation_and_dedupe`
   - Lê os JSONs do Bronze
   - Deduplica: **1 arquivo por cidade** usando `run_ts` maior
   - (Opcional) valida presença de colunas esperadas

2. `02_silver_hourly_weather_transform`
   - Flatten do `response.hourly` (arrays → linhas)
   - Tipagem (double/timestamp)
   - Grava **Silver** em Parquet

3. `03_gold_daily_weather_aggregation`
   - Agrega por cidade/dia
   - Grava **Gold** em Parquet

4. `04_data_quality_report`
   - Checks de consistência
   - Gera `dq_report.json`

> Versão enxuta (2 notebooks):

- `01_bronze_to_silver_weather_lakehouse`
- `02_silver_to_gold_and_dq_weather_lakehouse`

---

## 🚀 Como rodar (Databricks)

### 1) Conferir se os JSONs estão no Volume

No notebook (Python):

```python
dt = "2026-02-25"
bronze_dir = f"dbfs:/Volumes/weather/weather/raw/bronze/openmeteo/dt={dt}/"
display(dbutils.fs.ls(bronze_dir))

Se listar os .json, segue.

2) Bronze → (dedupe) → Silver (hourly)

Deduplica por city_id usando o maior run_ts

Faz flatten do response.hourly

Observação importante (Unity Catalog):

Não use input_file_name() em volumes UC.

Se precisar do nome do arquivo, use _metadata.file_name/_metadata.file_path.

3) Silver → Gold (daily)

Agrega por:

date, city_id, city_name, uf

Métricas:

avg_temp

max_wind

total_precip

avg_humidity

4) Data Quality

Checks aplicados:

null_time_utc == 0

null_city_id == 0

temperatura fora do range: < -20 ou > 55

umidade fora do range: < 0 ou > 100

precipitação negativa

vento negativo

Saída:

/Volumes/weather/weather/curated/quality/reports/dt=YYYY-MM-DD/dq_report.json
✅ Resultado esperado

Ao final, você terá:

Silver Parquet (hourly)

Gold Parquet (daily)

dq_report.json com ok: true/false

🔐 Boas práticas aplicadas

Particionamento por data (dt=YYYY-MM-DD)

Deduplicação por cidade usando run_ts mais recente

Outputs em Parquet (ótimo para consumo analítico)

Separação clara por camadas (Bronze/Silver/Gold)

Data Quality automatizado com relatório versionável

🔗 Repositórios

Ingestão / Azure Functions (HTTP → Bronze no Azure Storage):
https://github.com/GabrielContesini/weather-lakehouse
```
