import json


BUCKET_NAME = 'football-results-raw-041'
REGION_NAME = 'eu-north-1'
GLUE_CRAWLER_CONFIG = json.dumps({
        "Name": "football-crawler",
        "Role": "arn:aws:iam::905418173427:role/AirflowFootballResultsGlueCrawler",
        "DatabaseName": "football-results",
        "Targets": {"S3Targets": [{"Path": "football-results-transformed-041"}]},
})