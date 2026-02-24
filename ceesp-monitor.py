import pandas as pd
import requests
import os
from datetime import datetime
import smtplib
from email.message import EmailMessage

CSV_URL = "https://public.tableau.com/app/profile/has8400/viz/Contributionpatient/Tableaudebord5?:showVizHome=no&:format=csv"
HISTORY_FILE = "history.csv"

def load_data():
    df = pd.read_csv(CSV_URL)
    return df[[
        "Nom commercial",
        "Dénomination commune internationale",
        "Indication thérapeutique",
        "Date de publication CEESP"
    ]]

def load_history():
    if os.path.exists(HISTORY_FILE):
        return pd.read_csv(HISTORY_FILE)
    return pd.DataFrame()

def send_email(rows):
    msg = EmailMessage()
    msg["Subject"] = "[ALERTE CEESP] Nouveaux avis"
    msg["From"] = os.environ["SMTP_FROM"]
    msg["To"] = os.environ["EMAIL_TO"]

    body = "Nouveaux avis CEESP:\n\n"
    for _, r in rows.iterrows():
        body += f"{r['Nom commercial']} | {r['Dénomination commune internationale']} | {r['Date de publication CEESP']}\n"

    msg.set_content(body)

    with smtplib.SMTP("smtp.office365.com", 587) as s:
        s.starttls()
        s.login(os.environ["SMTP_USER"], os.environ["SMTP_PASS"])
        s.send_message(msg)

def main():
    df = load_data()
    old = load_history()

    key = lambda r: f"{r['Nom commercial']}|{r['Dénomination commune internationale']}|{r['Indication thérapeutique']}"
    new_keys = set(df.apply(key, axis=1))
    old_keys = set(old.apply(key, axis=1)) if not old.empty else set()

    diff = df[df.apply(key, axis=1).isin(new_keys - old_keys)]

    if not diff.empty:
        send_email(diff)

    df.to_csv(HISTORY_FILE, index=False)

if __name__ == "__main__":
    main()
