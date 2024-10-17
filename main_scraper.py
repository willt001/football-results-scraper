from curl_cffi import requests
from datetime import date, timedelta
from pydantic import BaseModel
from typing import List
import json
import os

class MatchResult(BaseModel):
    id: str
    competitors: list
    date: str
    link: str
    teams: list
    venue: dict
    status: dict
    attnd: str
    atVs: dict
    tableCaption: str

def new_session() -> requests.Session:
    session = requests.Session(impersonate="chrome")
    return session

def date_range(start_date: date, end_date: date) -> List[date]:
    if start_date > end_date:
        raise Exception('Start date must be less than or equal to end date.')
    delta =  (end_date - start_date).days
    return [start_date + timedelta(no_days) for no_days in range(delta + 1)]

def results_api(session: requests.Session, fixture_date: date) -> List[MatchResult]:
    datenum = fixture_date.strftime('%Y%m%d')
    url = f'https://www.espn.co.uk/football/fixtures/_/date/{datenum}?_xhr=pageContent'
    response = session.get(url)
    response.raise_for_status()
    data = response.json()
    result = []
    for league in data['events']:
        for match in league:
            result.append(MatchResult(**match))
    return result

def dump_results(results: List[MatchResult], fixture_date: date, output_root_path: str = './results') -> None:
    datenum = fixture_date.strftime('%Y%m%d')
    filepath = f'{output_root_path}/d={datenum}'
    if not os.path.exists(filepath):
        os.makedirs(filepath)
    with open(f'{filepath}/results.json', 'w') as f:
        for result in results:
            json.dump(result.model_dump(), f)
            f.write('\n')

if __name__ == "__main__":
    session = new_session()
    start_date, end_date = date(2024, 10, 12), date(2024, 10, 13)
    fixture_dates = date_range(start_date, end_date)
    for fixture_date in fixture_dates:
        results = results_api(session=session, fixture_date=fixture_date)
        dump_results(results=results, fixture_date=fixture_date)



