import csv
from datetime import datetime, timedelta

import requests
from robocorp import vault
from robocorp.tasks import task

HOURS = 24


@task
def pull_control_room_data():
    data_last_x_hours = fetch_data_within_last_hours(HOURS)
    write_to_csv(data_last_x_hours, f"data_last_{HOURS}_hours.csv")


def fetch_data_within_last_hours(hours):
    secrets = vault.get_secret("CR_process_runs")
    url = f"https://cloud.robocorp.com/api/v1/workspaces/{secrets['workspace_id']}/process-runs?process_id={secrets['process_id']}"
    headers = {"Content-Type": "application/json", "Authorization": secrets["api_key"]}
    results = []
    has_more = True
    while has_more:
        response = requests.get(url, headers=headers)
        data = response.json()
        for item in data["data"]:
            started_at = datetime.fromisoformat(item["started_at"].rstrip("Z"))
            if datetime.utcnow() - started_at > timedelta(hours=hours):
                has_more = False
                break
            results.append(item)
        # Move to the next page if one exists
        if has_more and data["has_more"]:
            url = data["next"]
        else:
            has_more = False
    return results


def write_to_csv(data, filename):
    if data:
        fields = data[0].keys()

        with open(filename, "w", newline="") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fields)
            writer.writeheader()
            for row in data:
                writer.writerow(row)
