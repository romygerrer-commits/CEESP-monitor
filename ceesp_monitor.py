import os
import time

import pandas as pd
import requests

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By


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


def extract_rows(driver):

    rows = []

    # IMPORTANT :
    # Tableau stocke les cellules
    # ligne par ligne dans aria-colindex

    cells = driver.find_elements(
        By.CSS_SELECTOR,
        '[role="gridcell"]'
    )

    current_row = {}

    for cell in cells:

        try:

            text = cell.text.strip()

            if not text:

                continue

            col_index = cell.get_attribute(
                "aria-colindex"
            )

            row_index = cell.get_attribute(
                "aria-rowindex"
            )

            if (
                not col_index
                or not row_index
            ):

                continue

            col_index = int(col_index)

            row_index = int(row_index)

            if row_index not in current_row:

                current_row[row_index] = {}

            current_row[row_index][
                col_index
            ] = text

        except Exception:

            pass

    for row_idx in sorted(current_row.keys()):

        row = current_row[row_idx]

        # colonnes :
        # 1 = nom
        # 2 = dci
        # 3 = indication
        # 4 = date

        if 1 not in row:

            continue

        nom = row.get(1, "")

        dci = row.get(2, "")

        indication = row.get(3, "")

        date = row.get(4, "")

        # ignore header

        if (
            "nom co"
            in nom.lower()
        ):

            continue

        # ignore lignes vides

        if (
            not nom
            or not dci
        ):

            continue

        rows.append({

            "nom commercial":
                nom,

            "dci":
                dci,

            "indication":
                indication,

            "date":
                date,

            "lien":
                ""
        })

    return rows


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

    print("Scrolling Tableau table...")

    all_rows = []

    seen = set()

    for _ in range(30):

        rows = extract_rows(driver)

        for row in rows:

            key = (
                row["nom commercial"]
                + "|"
                + row["dci"]
            )

            if key not in seen:

                seen.add(key)

                all_rows.append(row)

        print(
            f"{len(all_rows)} rows collected"
        )

        driver.execute_script(
            "window.scrollBy(0, 1500);"
        )

        time.sleep(1.5)

    driver.quit()

    if not all_rows:

        raise Exception(
            "No CEESP rows extracted"
        )

    df = pd.DataFrame(all_rows)

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
        lambda r: make_key(
            r,
            col_map
        ),
        axis=1
    )

    if os.path.exists(HISTORY_FILE):

        old = pd.read_csv(HISTORY_FILE)

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
