import pandas as pd
import requests
import os
from io import StringIO

CSV_URL = "https://public.tableau.com/views/Contributionpatient/Tableaudebord5?:showVizHome=no&:format=csv"

TEAMS_WEBHOOK = os.environ["TEAMS_WEBHOOK"]
HISTORY_FILE = "history.csv"


def normalize_col(col):
    return str(col).strip().lower()


def normalize_text(text):
    if pd.isna(text):
        return ""
    return str(text).strip()


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
    df.columns = [normalize_col(c) for c in df.columns]

    return df


def detect_columns(df):
    col_map = {}

    for col in df.columns:
        if "nom commercial" in col:
            col_map["nom"] = col
        elif "commune internationale" in col:
            col_map["dci"] = col
        elif "indication" in col:
            col_map["indication"] = col
        elif "validation" in col:
            col_map["date"] = col
        elif "lien" in col:
            col_map["lien"] = col

    required = ["nom", "dci", "indication"]

    for r in required:
        if r not in col_map:
            print("Columns available:", df.columns.tolist())
            raise Exception(f"Missing required column: {r}")

    return col_map


def make_key(row, col_map):
    return (
        normalize_text(row[col_map["nom"]]) + "|" +
        normalize_text(row[col_map["dci"]]) + "|" +
        normalize_text(row[col_map["indication"]])
    )


from datetime import datetime

def send_teams(rows, col_map):
    if rows.empty:
        return

    count = len(rows)

    

    if count > 1:
        text = "🏛️ **Nouveaux avis CEESP détectés**\n\n"
        text += f"{count} nouveaux avis publiés\n\n"
        text += "\n\u200b\n"
    else:
        text = "🏛️ **Nouvel avis CEESP détecté**\n\n"
        text += "1 nouvel avis publié\n\n"
        text += "\n\u200b\n"

    for i, (_, r) in enumerate(rows.iterrows(), 1):
        text += "\n\u200b\n"
        nom = normalize_text(r[col_map['nom']]).upper()
        url = normalize_text(r[col_map['lien']]) if "lien" in col_map else ""
        text += f"1️⃣️[{nom}]({url})\n\n" if url else f"1️⃣️{nom}\n\n"
        text += f"• DCI : {normalize_text(r[col_map['dci']])}\n\n"
        text += f"• Indication : {normalize_text(r[col_map['indication']])}\n\n"

        if "date" in col_map:
            text += f"• Date de validation : {normalize_text(r[col_map['date']])}\n\n"

        text += "\n\u200b\n"

    text += "\n\u200b\n"
    text += "🔎 Tableau de bord complet :\n"
    text += "https://public.tableau.com/views/Contributionpatient/Tableaudebord5\n"

    payload = {"text": text}
    requests.post(TEAMS_WEBHOOK, json=payload)


def main():
    df = load_data()
    col_map = detect_columns(df)

    if os.path.exists(HISTORY_FILE):
        old = pd.read_csv(HISTORY_FILE)
        old_keys = set(old["key"])
    else:
        old_keys = set()

    df["key"] = df.apply(lambda r: make_key(r, col_map), axis=1)

    new_rows = df[~df["key"].isin(old_keys)]

    if not new_rows.empty:
        print(f"{len(new_rows)} new CEESP rows detected")
        send_teams(new_rows, col_map)
    else:
        print("No new CEESP entries")

    df.to_csv(HISTORY_FILE, index=False)


if __name__ == "__main__":
    main()
