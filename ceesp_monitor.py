import pandas as pd
import requests
import os
from datetime import datetime

CSV_URL = "https://public.tableau.com/views/Contributionpatient/Tableaudebord5?:showVizHome=no&:format=csv"

TEAMS_WEBHOOK = os.environ["TEAMS_WEBHOOK"]

HISTORY_FILE = "history.csv"


def load_data():
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "text/csv",
        "Referer": "https://public.tableau.com/views/Contributionpatient/Tableaudebord5"
    }

    r = requests.get(CSV_URL, headers=headers, timeout=30)
    r.raise_for_status()

    if "<html" in r.text.lower():
        print("Received HTML instead of CSV")
        print(r.text[:500])
        raise Exception("Tableau returned HTML instead of CSV")

    df = pd.read_csv(pd.io.common.StringIO(r.text))
    return df


def make_key(row):
    return f"{row['Nom commercial']}|{row['Dénomination commune internationale']}|{row['Indication thérapeutique']}"


def send_teams(rows):
    if rows.empty:
        return

    text = "Nouveaux avis CEESP:\n\n"

    for _, r in rows.iterrows():
        text += f"Nom commercial: {r['Nom commercial']}\n"
        text += f"DCI: {r['Dénomination commune internationale']}\n"
        text += f"Indication: {r['Indication thérapeutique']}\n"
        text += f"Date: {r['Date de publication CEESP']}\n"
        text += "------------------------\n"

    payload = {"text": text}
    requests.post(TEAMS_WEBHOOK, json=payload)


def main():
    df = load_data()

    if os.path.exists(HISTORY_FILE):
        old = pd.read_csv(HISTORY_FILE)
    else:
        old = pd.DataFrame()

    if not old.empty:
        old_keys = set(old.apply(make_key, axis=1))
    else:
        old_keys = set()

    df["key"] = df.apply(make_key, axis=1)

    new_rows = df[~df["key"].isin(old_keys)]

    if not new_rows.empty:
        print(f"{len(new_rows)} new CEESP rows detected")
        send_teams(new_rows)

    df.drop(columns=["key"], inplace=True)
    df.to_csv(HISTORY_FILE, index=False)


if __name__ == "__main__":
    main()
