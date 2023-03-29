# Final Project

## Project description
**Comment for reviewer**: Привет! Спасибо за твои комментарии - все поправила:) Единственная проблема - в S3 автор загрузил данные только за первую часть одного из дней, поэтому и в финальной витрине, и в дэше всего крайне мало. Наш куратор сказал сдавать так, потому что до автора проекта пару недель уже не достучаться. Мой куратор - Олег Васильев.

### Intro
The project is based on the FinTech Startap data, which offers international banking services through the application: users can safely transfer money to different countries.

The company adheres to a decentralized financial system: in each country where the application is available, there is a separate service working with the currency of this country. At the same time, the company keeps records of the transactional activity of customers inside and between the countries: a single data transfer protocol has been developed, which provides the same table structure in all countries.

**The goal** is to understand the company's turnover dynamics and identify what leads to its changes.

#### Data storages:
* S3;
* PostgreSQL;
* Kafka Spark Streaming;

## Steps
* Source: S3 -> load data in staging (Vertica)
* Prepare data and load it to DWH
* Prepare visualization via Metabase

### /scr/dags:
* 1_data_import: to load data from S3 to Vertica
* 2_datamart_update: prepare data insert in DWH

### /scr/sql:
* final_project_staging - tables DDL in staging 
* final_project_dwh - final view DDL
* global_metrics_view - insert DML for the 2_datamart_update dag


**Final view**: MARINAARYAMNOVAMAILRU__DWH.global_metrics
