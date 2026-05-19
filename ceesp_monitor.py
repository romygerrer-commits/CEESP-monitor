import os
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
        "--window-size=1920,3000"
    )

    driver = webdriver.Chrome(
        options=chrome_options
    )

    print("Opening Tableau dashboard...")

    driver.get(TABLEAU_URL)

    time.sleep(20)

    print("Extracting Tableau internal data...")

    data = driver.execute_script(
        """
        const results = [];

        const allObjects = Object.values(window);

        for (const obj of allObjects) {

            try {

                if (
                    obj &&
                    obj._sheetImpl &&
                    obj._sheetImpl._columns
                ) {

                    const sheet =
                        obj._sheetImpl;

                    const columns =
                        sheet._columns;

                    const rows =
                        sheet._data;

                    results.push({
                        columns: columns,
                        rows: rows
                    });
                }

            } catch(e) {}
        }

        return results;
        """
    )

    driver.quit()

    if not data:

        raise Exception(
            "No Tableau data found"
        )

    print(
        f"{len(data)} Tableau objects found"
    )

    best_rows = []

    for obj in data:

        try:

            columns = obj["columns"]

            rows = obj["rows"]

            if not rows:

                continue

            if len(rows) < len(best_rows):

                continue

            best_rows = rows

        except Exception:

            pass

    if not best_rows:

        raise Exception(
            "No Tableau rows found"
        )

    structured_rows = []

    for row in best_rows:

        try:

            structured_rows.append({

                "nom commercial":
                    row[0],

                "dci":
                    row[1],

                "indication":
                    row[2],

                "date":
                    row[3],

                "lien":
                    ""
            })

        except Exception:

            pass

    if not structured_rows:

        raise Exception(
            "No structured rows parsed"
        )

    df = pd.DataFrame(
        structured_rows
    )

    df.columns = [
        normalize_col(c)
        for c in df.columns
    ]

    print(
        f"{len(df)} rows loaded"
    )

    print(df.head(20))

    return df


def detect_columns(df):

    return {
        "nom": "nom commercial",
        "dci": "dci",
        "indication": "indication",
        "date": "date",
        "lien": "lien"
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
