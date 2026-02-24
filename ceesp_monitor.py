import pandas as pd
import requests
import os

CSV_URL = "https://public.tableau.com/app/profile/has8400/viz/Contributionpatient/Tableaudebord5?:showVizHome=no&:format=csv"
HISTORY_FILE = "history.csv"

def load_data():
    return pd.read_csv(CSV_URL)

def load_history():
    try:
        return pd.read_csv(HISTORY_FILE)
    except:
        return pd.DataFrame()

def send_teams(rows):
    webhook = os.environ["TEAMS_WEBHOOK"]

    text = "🚨 **Nouveaux avis CEESP détectés**\n\n"
    for _, r in rows.iterrows():
        text += f"- **{r.get('Nom commercial','')}** | {r.get('Indication thérapeutique','')} | {r.get('Date de publication CEESP','')}\n"

    payload = {"text": text}
    requests.post(webhook, json=payload)

def main():
    df = load_data()
    old = load_history()

    # clé unique par ligne
    key = lambda r: str(r.values.tolist())
    new_keys = set(df.apply(key, axis=1))
    old_keys = set(old.apply(key, axis=1)) if not old.empty else set()

    diff = df[df.apply(key, axis=1).isin(new_keys - old_keys)]

    if not diff.empty:
        send_teams(diff)

    df.to_csv(HISTORY_FILE, index=False)

if __name__ == "__main__":
    main()
