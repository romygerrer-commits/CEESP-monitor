import pandas as pd
import requests
import os
from io import StringIO
from datetime import datetime

CSV_URL = "https://public.tableau.com/views/Contributionpatient/Tableaudebord5?:showVizHome=no&:format=csv"

TEAMS_WEBHOOK = os.environ.get("TEAMS_WEBHOOK")
HISTORY_FILE = "history.csv"


# -----------------------------
# Normalisation helpers
# -----------------------------
def normalize_col(col):
    return str(col).strip().lower()


def normalize_text(text):
    if pd.isna(text):
        return ""
    return str(text).strip()


# -----------------------------
# Load Tableau data
# -----------------------------
def load_data():

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "text/csv",
        "Referer": "https://public.tableau.com/views/Contributionpatient/Tableaudebord5"
    }

    print("Downloading Tableau CSV...")

    r = requests.get(CSV_URL, headers=headers, timeout=60)
    r.raise_for_status()

    if not r.text.strip():
        raise Exception("Downloaded CSV is empty")

    if "<html" in r.text.lower():
        raise Exception("Tableau returned HTML instead of CSV")

    df = pd.read_csv(StringIO(r.text))

    if df.empty:
        raise Exception("CSV loaded but dataframe is empty")

    df.columns = [normalize_col(c) for c in df.columns]

    print(f"{len(df)} rows loaded")

    return df


# -----------------------------
# Detect column names dynamically
# -----------------------------
def detect_columns(df):

    col_map = {}

    for col in df.columns:

        if "nom commercial" in col:
            col_map["nom"] = col

        elif "commune internationale" in col or "dci" in col:
            col_map["dci"] = col

        elif "indication" in col:
            col_map["indication"] = col

        elif "validation" in col:
            col_map["date"] = col

    required = ["nom", "dci", "indication"]

    for r in required:
        if r not in col_map:
            print("Available columns:", df.columns.tolist())
            raise Exception(f"Missing required column: {r}")

    return col_map


# -----------------------------
# Unique key
# -----------------------------
def make_key(row, col_map):

    return (
        normalize_text(row[col_map["nom"]]) + "|" +
        normalize_text(row[col_map["dci"]]) + "|" +
        normalize_text(row[col_map["indication"]])
    )


# -----------------------------
# Load history safely
# -----------------------------
def load_history():

    if not os.path.exists(HISTORY_FILE):
        print("No history file found (first run)")
        return set()

    if os.path.getsize(HISTORY_FILE) == 0:
        print("History file empty (first run)")
        return set()

    try:

        df = pd.read_csv(HISTORY_FILE)

        if "key" not in df.columns:
            print("History file has no key column")
            return set()

        print(f"{len(df)} historical records loaded")

        return set(df["key"])

    except Exception as e:

        print("History load failed:", e)
        return set()


# -----------------------------
# Save history safely
# -----------------------------
def save_history(df):

    df.to_csv(HISTORY_FILE, index=False)

    print(f"{len(df)} records saved to history")


# -----------------------------
# Send Teams notification
# -----------------------------
def send_teams(rows, col_map):

    if not TEAMS_WEBHOOK:
        print("TEAMS_WEBHOOK not set, skipping notification")
        return

    if rows.empty:
        return

    count = len(rows)

    if count == 1:
        text = "🏛️ **Nouvel avis CEESP détecté**\n\n"
    else:
        text = "🏛️ **Nouveaux avis CEESP détectés**\n\n"

    text += f"{count} nouvel(s) avis publié(s)\n\n"

    for _, r in rows.iterrows():

        text += "\n\u200b\n"

        text += f"💊 **{normalize_text(r[col_map['nom']]).upper()}**\n\n"

        text += f"• DCI : {normalize_text(r[col_map['dci']])}\n\n"

        text += f"• Indication : {normalize_text(r[col_map['indication']])}\n\n"

        if "date" in col_map:
            text += f"• Date de validation : {normalize_text(r[col_map['date']])}\n\n"

    text += "\n🔎 Tableau de bord complet :\n"
    text += "https://public.tableau.com/views/Contributionpatient/Tableaudebord5\n"

    payload = {"text": text}

    response = requests.post(TEAMS_WEBHOOK, json=payload)

    print("Teams notification sent:", response.status_code)


# -----------------------------
# Main logic
# -----------------------------
def main():

    print("Starting CEESP monitor")

    df = load_data()

    col_map = detect_columns(df)

    df["key"] = df.apply(lambda r: make_key(r, col_map), axis=1)

    old_keys = load_history()

    new_rows = df[~df["key"].isin(old_keys)]

    if len(old_keys) == 0:
        print("First run detected — saving baseline without notification")
    else:

        if not new_rows.empty:

            print(f"{len(new_rows)} new entries detected")

            send_teams(new_rows, col_map)

        else:

            print("No new entries")

    save_history(df)

    print("Finished successfully")


# -----------------------------
if __name__ == "__main__":
    main()
