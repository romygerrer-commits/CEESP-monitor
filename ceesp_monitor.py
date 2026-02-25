import pandas as pd
import requests
import os
from io import StringIO

CSV_URL = "https://public.tableau.com/app/profile/has8400/viz/Contributionpatient/Tableaudebord5?:showVizHome=no&:format=csv"
HISTORY_FILE = "history.csv"

def load_data():
    headers = {"User-Agent": "Mozilla/5.0"}
    r = requests.get(CSV_URL, headers=headers)
    r.raise_for_status()

    df = pd.read_csv(
        StringIO(r.text),
        sep=",",
        engine="python",
        quotechar='"',
        on_bad_lines="skip"
    )

    # On garde uniquement les colonnes utiles (si elles existent)
    expected_cols = [
        "Nom commercial",
        "Dénomination commune internationale",
        "Indication thérapeutique",
        "Date de publication CEESP"
    ]

    df = df[[c for c in expected_cols if c in df.columns]]

    return df

def load_history():
    try:
        return pd.read_csv(HISTORY_FILE)
    except:
        return pd.DataFrame()

def send_teams(rows):
    webhook = os.environ["TEAMS_WEBHOOK"]

    text = "🚨 **Nouveaux avis CEESP détectés**\n\n"

    for _, r in rows.iterrows():
        text += f"**{r['Nom commercial']}**\n"
        text += f"DCI : {r['Dénomination commune internationale']}\n"
        text += f"Indication : {r['Indication thérapeutique']}\n"
        text += f"Date : {r['Date de publication CEESP']}\n"
        text += "--------------------------\n"

    requests.post(webhook, json={"text": text})

def main():
    df = load_data()
    old = load_history()

    df = df.fillna("").astype(str)

    def key(row):
        return "|".join(row.values.tolist())

    new_keys = set(df.apply(key, axis=1))
    old_keys = set(old.apply(key, axis=1)) if not old.empty else set()

    diff = df[df.apply(key, axis=1).isin(new_keys - old_keys)]

    if not diff.empty:
        print(f"{len(diff)} new CEESP rows detected")
        send_teams(diff)
    else:
        print("No new CEESP entries")

    df.to_csv(HISTORY_FILE, index=False)

if __name__ == "__main__":
    main()
