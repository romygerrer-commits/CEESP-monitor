import os
import re
import time

import pandas as pd
import requests

from selenium import webdriver
from selenium.webdriver.chrome.options import Options


TABLEAU_URL = (
    "https://public.tableau.com/views/"
    "Contributionpatient/Tableaudebord5?:showVizHome=no"
)

TEAMS_WEBHOOK = os.environ["TEAMS_WEBHOOK"]

HISTORY_FILE = "history.csv"


MONTHS = [
    "Jan", "Feb", "Mar", "Apr",
    "May", "Jun", "Jul", "Aug",
    "Sep", "Oct", "Nov", "Dec"
]


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


def is_date_line(text):

    for m in MONTHS:

        if text.startswith(m):

            return True

    return False


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
        "--window-size=1920,5000"
    )

    driver = webdriver.Chrome(
        options=chrome_options
    )

    print("Opening Tableau dashboard...")

    driver.get(TABLEAU_URL)

    time.sleep(20)

    print("Extracting visible text...")

    body_text = driver.find_element(
        "tag name",
        "body"
    ).text

    driver.quit()

    raw_lines = [

        line.strip()

        for line in body_text.split("\n")

        if line.strip()
    ]

    print(
        f"{len(raw_lines)} raw lines"
    )

    excluded = [

        "nom co",
        "dénomination",
        "indication courte",
        "validation",
        "pathologie"

    ]

    lines = []

    for line in raw_lines:

        lower = line.lower()

        if any(
            x in lower
            for x in excluded
        ):

            continue

        lines.append(line)

    print(
        f"{len(lines)} cleaned lines"
    )

    # détecte les dates

    date_lines = [
        x for x in lines
        if is_date_line(x)
    ]

    n_rows = len(date_lines)

    print(
        f"Detected {n_rows} rows"
    )

    if n_rows == 0:

        raise Exception(
            "Could not detect rows"
        )

    # reconstruction colonnes

    col_nom = lines[0:n_rows]

    col_dci = lines[
        n_rows:n_rows * 2
    ]

    col_indication = lines[
        n_rows * 2:n_rows * 3
    ]

    col_date = lines[
        n_rows * 3:n_rows * 4
    ]

    rows = []

    for i in range(n_rows):

        try:

            rows.append({

                "nom commercial":
                    col_nom[i],

                "dci":
                    col_dci[i],

                "indication":
                    col_indication[i],

                "date":
                    col_date[i],

                "lien":
                    ""
            })

        except Exception:

            pass

    if not rows:

        raise Exception(
            "No rows reconstructed"
        )

    df = pd.DataFrame(rows)

    df.columns = [
        normalize_col(c)
        for c in df.columns
    ]

    print(df.head(20))

    return df


def detect_columns(df):

    return {

        "nom":
            "nom commercial",

        "dci":
            "dci",

        "indication":
            "indication",

        "date":
            "date",

        "lien":
            "lien"
    }


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

        text += (
            f"• Date de validation : "
            f"{format_date_fr(row[col_map['date']])}\n\n"
        )

        text += "\n"

    payload = {
        "text": text
    }

    requests.post(
        TEAMS_WEBHOOK,
        json=payload,
        timeout=30
    )


def main():

    print("Starting CEESP monitor")

    df = load_data()

    col_map = detect_columns(df)

    df["key"] = df.apply(
        lambda r: make_key(
            r,
            col_map
        ),
        axis=1
    )

    if os.path.exists(HISTORY_FILE):

        old = pd.read_csv(
            HISTORY_FILE
        )

        if "key" in old.columns:

            old_keys = set(
                old["key"]
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
            f"{len(new_rows)} new rows detected"
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
