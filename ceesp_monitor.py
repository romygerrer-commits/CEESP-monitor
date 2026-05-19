import os
import re
import json
import requests
import pandas as pd

from io import StringIO


TABLEAU_URL = (
    "https://public.tableau.com/views/"
    "Contributionpatient/Tableaudebord5?:showVizHome=no"
)

TEAMS_WEBHOOK = os.environ["TEAMS_WEBHOOK"]

HISTORY_FILE = "history.csv"


HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    )
}


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

            return date_obj.strftime("%d/%m/%Y")

    except Exception:

        pass

    return normalize_text(value)


def get_session_info():

    print("Opening Tableau page...")

    r = requests.get(
        TABLEAU_URL,
        headers=HEADERS,
        timeout=60
    )

    r.raise_for_status()

    text = r.text

    sessionid_match = re.search(
        r'sessionid":"([^"]+)"',
        text
    )

    sheet_match = re.search(
        r'"sheetId":"([^"]+)"',
        text
    )

    if not sessionid_match:

        bootstrap_match = re.search(
            r'bootstrapSession/sessions/([A-Z0-9\-]+)',
            text
        )

        if bootstrap_match:

            sessionid = bootstrap_match.group(1)

        else:

            raise Exception(
                "Could not find Tableau session ID"
            )

    else:

        sessionid = sessionid_match.group(1)

    if sheet_match:

        sheet_id = sheet_match.group(1)

    else:

        sheet_id = "Tableaudebord5"

    print(f"Session ID: {sessionid}")

    return sessionid, sheet_id


def load_data():

    sessionid, sheet_id = get_session_info()

    csv_url = (
        f"https://public.tableau.com/vizql/"
        f"wb/bootstrapSession/sessions/{sessionid}"
    )

    payload = {
        "worksheetPortSize": '{"w":1365,"h":635}',
        "dashboardPortSize": '{"w":1365,"h":635}',
        "clientDimension": '{"w":1365,"h":635}',
        "sheet_id": sheet_id,
        "showParams": '{"checkpoint":false}',
    }

    print("Requesting Tableau bootstrap session...")

    r = requests.post(
        csv_url,
        data=payload,
        headers=HEADERS,
        timeout=60
    )

    r.raise_for_status()

    text = r.text

    csv_match = re.search(
        r'"presModelMap":"(.*?)"',
        text,
        re.DOTALL
    )

    if not csv_match:

        raise Exception(
            "Could not parse Tableau response"
        )

    print("Downloading underlying CSV...")

    direct_csv = (
        "https://public.tableau.com/views/"
        "Contributionpatient/Tableaudebord5.csv"
        "?:showVizHome=no"
    )

    csv_response = requests.get(
        direct_csv,
        headers=HEADERS,
        timeout=60
    )

    csv_response.raise_for_status()

    if "<html" in csv_response.text.lower():

        raise Exception(
            "Tableau returned HTML instead of CSV"
        )

    df = pd.read_csv(
        StringIO(csv_response.text)
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

            raise Exception(
                f"Missing required column: {req}"
            )

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
            "🏛️ **Nouveaux avis CEESP détectés**\n\n"
            f"{count} nouveaux avis publiés\n\n"
        )

    else:

        text = (
            "🏛️ **Nouvel avis CEESP détecté**\n\n"
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

        old_df = pd.read_csv(HISTORY_FILE)

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

    if not new_rows.empty():

        print(
            f"{len(new_rows)} "
            f"new CEESP rows detected"
        )

        send_teams(
            new_rows,
            col_map
        )

    else:

        print("No new CEESP entries")

    df.to_csv(
        HISTORY_FILE,
        index=False
    )

    print("History updated")


if __name__ == "__main__":

    main()
