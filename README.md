## Football Results Scraper

Scraping historical football results from ESPN, transforming with PySpark in AWS Glue, and loading to data lake on AWS S3. 

The web scraping approach used is sending requests directly to ESPN's hidden backend API, rather than parsing data from HTML content with BeautifulSoup.

The main_scraper.py file can be run locally without any AWS architecture to demonstrate the raw JSON data retrieved by the scraper:

Create a virtual environment.
```bash
python -m venv .venv
```
Activate the virtual environment.
```bash
.venv/scripts/activate
```
Install the dependencies for the script.
```bash
pip install curl_cffi pydantic
```
Run the script.
```bash
python main_scraper.py
```
Results will be saved to the following directory in JSON lines format by default :
```bash
./results/d=20240108
```

## Airflow DAGs

The DAGs can be viewed locally, but require the respective S3, Glue and IAM architecture to be configured in AWS to run the pipeline. As well as the Airflow connections and constants.py file.

```bash
docker compose up -d --build
```

### ETL DAG

![image](https://github.com/user-attachments/assets/0d8a2387-1e6a-432d-9d35-56fca691418c)

### Glue Crawler DAG

![image](https://github.com/user-attachments/assets/f052a8c7-1956-47f2-b680-afe192912db6)

The Glue Crawler DAG will get run after the ETL DAG finishes (if no failures). This is responsible for updating the data catalog so that the data can be used in other services such as AWS Athena, for example:

```bash
select
    fixture_date,
    league_name,
    flag_cancelled,
    home_team,
    home_score,
    away_team,
    away_score,
    final_score,
    attendance,
    venue_name,
    country
from 
    football_results 
where 
    weekstarting >= 20241001
limit 10
```
![image](https://github.com/user-attachments/assets/1ae58618-fc39-4042-9fba-9debc97befd6)


