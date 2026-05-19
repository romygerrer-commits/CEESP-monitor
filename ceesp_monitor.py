import os
import re
import time
import json
import pandas as pd
import requests

from io import StringIO

from selenium import webdriver
from selenium.webdriver.chrome.options import Options


TABLEAU_URL = (
    "https://public.tableau.com/views/"
    "Contributionpatient/Tableaudebord5?:showVizHome=no"
)

TEAMS_WEBHOOK = os.environ["TEAMS_WEBHOOK"]

HISTORY_FILE = "history.csv"


def normalize_col(col):

    return str(col).strip().lower()


def normalize_text(text):

    if pd.isna(text):
        return ""

    return str(text).strip()


def format_date_fr(value):

    if pd.isna(value):
        return ""

    try:

        date_obj = pd.to_datetime(
            value,
            dayfirst=True,
            errors="coerce"
        )

        if pd.notna(date_obj):

            return date_obj.strftime(
                "%d/%m/%Y"
            )

    except Exception:

        pass

    return normalize_text(value)


def load_data():

    print("Launching Chrome...")

    chrome_options = Options()

    chrome_options.add_argument(
        "--headless=new"
    )

    chrome_options.add_argument(
        "--no-sandbox"
    )

    chrome_options.add_argument(
        "--disable-dev-shm-usage"
    )

    chrome_options.add_argument(
        "--disable-gpu"
    )

    chrome_options.add_argument(
        "--window-size=1920,1080"
    )

    chrome_options.set_capability(
        "goog:loggingPrefs",
        {"performance": "ALL"}
    )

    driver = webdriver.Chrome(
        options=chrome_options
    )

    print("Opening Tableau dashboard...")

    driver.get(TABLEAU_URL)

    time.sleep(20)

    logs = driver.get_log(
        "performance"
    )

    session_id = None

    for entry in logs:

        try:

            message = json.loads(
                entry["message"]
            )["message"]

            if (
                message["method"]
                == "Network.requestWillBeSent"
            ):

                url = message["params"][
                    "request"
                ]["url"]

                if (
                    "bootstrapSession/sessions/"
                    in url
                ):

                    match = re.search(
                        r"sessions/([^/]+)",
                        url
                    )

                    if match:

                        session_id = (
                            match.group(1)
                        )

                        print(
                            f"Session ID found: "
                            f"{session_id}"
                        )

                        break

        except Exception:

            pass

    driver.quit()

    if not session_id:

        raise Exception(
            "Could not find Tableau session ID"
        )

    csv_url = (
        "https://public.tableau.com"
        "/vizql/w/Contributionpatient"
        "/v/Tableaudebord5"
        "/bootstrapSession/sessions/"
        f"{session_id}"
        "/commands/tabdoc/getCsv"
    )

    headers = {
        "User-Agent": (
            "Mozilla/5.0"
        ),
        "Referer": TABLEAU_URL,
        "Accept": "*/*"
    }

    print("Downloading CSV...")

    response = requests.post(
        csv_url,
        headers=headers,
        timeout=60
    )

    response.raise_for_status()

    if "<html" in response.text.lower():

        raise Exception(
            "Received HTML instead of CSV"
        )

    if len(response.text) < 100:

        raise Exception(
            "CSV response too short"
        )

    df = pd.read_csv(
        StringIO(response.text)
    )

    df.columns = [
        normalize_col(c)
        for c in df.columns
    ]

    print(f"{len(df)} rows loaded")

    return df


def detect_columns(df):

    col_map = {}

    for col in df.columns:

        if "nom commercial" in col:

            col_map["nom"] = col

        elif (
            "commune internationale" in col
            or "dci" in col
        ):

            col_map["dci"] = col

        elif "indication" in col:

            col_map["indication"] = col

        elif (
            "validation" in col
            or "date" in col
        ):

            col_map["date"] = col

        elif (
            "lien" in col
            or "link" in col
            or "url" in col
        ):

            col_map["lien"] = col

    required = [
        "nom",
        "dci",
        "indication"
    ]

    for req in required:

        if req not in col_map:

            print(df.columns.tolist())

            raise Exception(
                f"Missing required column: "
                f"{req}"
            )

    print("Detected columns:")
    print(col_map)

    return col_map


def make_key(row, col_map):

    return (
        normalize_text(
            row[col_map["nom"]]
        )
        + "|"
        + normalize_text(
            row[col_map["dci"]]
        )
        + "|"
        + normalize_text(
            row[col_map["indication"]]
        )
    )


def send_teams(rows, col_map):

    if rows.empty:

        return

    count = len(rows)

    if count > 1:

        text = (
            "🏛️ **Nouveaux avis "
            "CEESP détectés**\n\n"
            f"{count} nouveaux avis "
            "publiés\n\n"
        )

    else:

        text = (
            "🏛️ **Nouvel avis "
            "CEESP détecté**\n\n"
            "1 nouvel avis publié\n\n"
        )

    for _, row in rows.iterrows():

        nom = normalize_text(
            row[col_map["nom"]]
        ).upper()

        text += f"💊 {nom}\n\n"

        text += (
            f"• DCI : "
            f"{normalize_text(row[col_map['dci']])}\n\n"
        )

        text += (
            f"• Indication : "
            f"{normalize_text(row[col_map['indication']])}\n\n"
        )

        if "date" in col_map:

            text += (
                f"• Date de validation : "
                f"{format_date_fr(row[col_map['date']])}\n\n"
            )

        text += "\n"

    text += (
        "🔎 Tableau de bord complet :\n"
        "https://public.tableau.com/views/"
        "Contributionpatient/Tableaudebord5\n"
    )

    payload = {
        "text": text
    }

    response = requests.post(
        TEAMS_WEBHOOK,
        json=payload,
        timeout=30
    )

    print(
        f"Teams notification sent "
        f"(status {response.status_code})"
    )


def main():

    print("Starting CEESP monitor")

    df = load_data()

    col_map = detect_columns(df)

    df["key"] = df.apply(
        lambda row: make_key(
            row,
            col_map
        ),
        axis=1
    )

    if os.path.exists(HISTORY_FILE):

        old_df = pd.read_csv(
            HISTORY_FILE
        )

        if "key" in old_df.columns:

            old_keys = set(
                old_df["key"]
            )

        else:

            old_keys = set()

    else:

        old_keys = set()

    new_rows = df[
        ~df["key"].isin(old_keys)
    ]

    if not new_rows.empty:

        print(
            f"{len(new_rows)} "
            f"new CEESP rows detected"
        )

        send_teams(
            new_rows,
            col_map
        )

    else:

        print(
            "No new CEESP entries"
        )

    df.to_csv(
        HISTORY_FILE,
        index=False
    )

    print("History updated")


if __name__ == "__main__":

    main()
