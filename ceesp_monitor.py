import pandas as pd
import requests
import os
import unicodedata
from io import StringIO

CSV_URL = "https://public.tableau.com/views/Contributionpatient/Tableaudebord5?:showVizHome=no&:format=csv"

TEAMS_WEBHOOK = os.environ["TEAMS_WEBHOOK"]
HISTORY_FILE = "history.csv"


def normalize(text):
    if pd.isna(text):
        return ""
    return str(text).strip()


def normalize_col(col):
    col = col.strip()
    col = unicodedata.normalize("NFKD", col)
    col = col.encode("ascii", "ignore").decode("utf-8")
    col = col.lower()
    return col


def load_data():
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "text/csv",
        "Referer": "https://public.tableau.com/views/Contributionpatient/Tableaudebord5"
    }

    r = requests.get(CSV_URL, headers=headers, timeout=30)
    r.raise_for_status()

    if "<html" in r.text.lower():
        raise Exception("Tableau returned HTML instead of CSV")

    df = pd.read_csv(StringIO(r.text))

    # Normalise column names
    df.columns = [normalize_col(c) for c in df.columns]

    return df


def map_columns(df):
    col_map = {}

    for col in df.columns:
        if "nom commercial" in col:
            col_map["nom"] = col
        elif "denomination" in col:
            col_map["dci"] = col
        elif "indication" in col:
            col_map["indication"] = col
        elif "date" in col:
            col_map["date"] = col

    required = ["nom", "dci", "indication"]

    for r in required:
        if r not in col_map:
            raise Exception(f"Missing column: {r}")

    return col_map


def make_key(row, col_map):
    return (
        normalize(row[col_map["nom"]]) + "|" +
        normalize(row[col_map["dci"]]) + "|" +
        normalize(row[col_map["indication"]])
    )


def send_teams(rows, col_map):
    if rows.empty:
        return

    text = "Nouveaux avis CEESP:\n\n"

    for _, r in rows.iterrows():
        text += f"Nom commercial: {normalize(r[col_map['nom']])}\n"
        text += f"DCI: {normalize(r[col_map['dci']])}\n"
        text += f"Indication: {normalize(r[col_map['indication']])}\n"

        if "date" in col_map:
            text += f"Date: {normalize(r[col_map['date']])}\n"

        text += "--------------------------\n"

    payload = {"text": text}
    requests.post(TEAMS_WEBHOOK, json=payload)


def main():
    df = load_data()
    col_map = map_columns(df)

    if os.path.exists(HISTORY_FILE):
        old = pd.read_csv(HISTORY_FILE)
        old_keys = set(old["key"])
    else:
        old = pd.DataFrame()
        old_keys = set()

    df["key"] = df.apply(lambda r: make_key(r, col_map), axis=1)

    new_rows = df[~df["key"].isin(old_keys)]

    if not new_rows.empty:
        print(f"{len(new_rows)} new CEESP rows detected")
        send_teams(new_rows, col_map)

    df.to_csv(HISTORY_FILE, index=False)


if __name__ == "__main__":
    main()
