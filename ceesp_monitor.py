import os
import time
import pandas as pd
import requests

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options


TABLEAU_URL = (
    "https://public.tableau.com/views/"
    "Contributionpatient/Tableaudebord5?:showVizHome=no"
)

TEAMS_WEBHOOK = os.environ["TEAMS_WEBHOOK"]

HISTORY_FILE = "history.csv"


def normalize_text(text):

    if text is None:
        return ""

    return str(text).strip()


def format_date(value):

    if pd.isna(value):
        return ""

    return normalize_text(value)


def setup_driver():

    chrome_options = Options()

    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,12000")

    driver = webdriver.Chrome(options=chrome_options)

    return driver


def scroll_table(driver):

    driver.execute_script("""
        const scrollables = Array.from(document.querySelectorAll('*'))
            .filter(el => el.scrollHeight > el.clientHeight);

        let biggest = null;
        let maxHeight = 0;

        for (const el of scrollables) {

            if (el.scrollHeight > maxHeight) {

                biggest = el;
                maxHeight = el.scrollHeight;
            }
        }

        if (biggest) {
            biggest.scrollTop += 800;
        }
    """)


def extract_rows(driver):

    rows_data = []

    seen = set()

    previous_count = 0
    stable_iterations = 0

    for i in range(200):

        time.sleep(2)

        rows = driver.find_elements(
            By.CSS_SELECTOR,
            "div[role='row']"
        )

        print(f"Iteration {i} | {len(rows)} HTML rows found")

        for row in rows:

            try:

                cells = row.find_elements(
                    By.CSS_SELECTOR,
                    "div[role='gridcell']"
                )

                values = []

                for cell in cells:

                    txt = normalize_text(cell.text)

                    if txt != "":
                        values.append(txt)

                # on veut exactement :
                # nom / dci / indication / date

                if len(values) < 4:
                    continue

                nom = values[0]
                dci = values[1]
                indication = values[2]
                date = values[3]

                key = (
                    nom
                    + "|"
                    + dci
                    + "|"
                    + indication
                )

                if key in seen:
                    continue

                seen.add(key)

                rows_data.append({

                    "nom commercial": nom,
                    "dci": dci,
                    "indication": indication,
                    "date": date,
                    "lien": ""

                })

            except Exception:
                pass

        current_count = len(rows_data)

        print(f"{current_count} rows collected")

        if current_count == previous_count:

            stable_iterations += 1

        else:

            stable_iterations = 0

        previous_count = current_count

        if stable_iterations >= 10:

            print("End of table reached")
            break

        scroll_table(driver)

    return rows_data


def load_data():

    print("Launching Chrome...")

    driver = setup_driver()

    print("Opening Tableau dashboard...")

    driver.get(TABLEAU_URL)

    time.sleep(20)

    print("Extracting rows from Tableau...")

    rows = extract_rows(driver)

    driver.quit()

    if len(rows) == 0:

        raise Exception(
            "No rows extracted from Tableau"
        )

    df = pd.DataFrame(rows)

    print(df.head(20))

    return df


def make_key(row):

    return (

        normalize_text(row["nom commercial"])
        + "|"
        + normalize_text(row["dci"])
        + "|"
        + normalize_text(row["indication"])

    )


def send_teams(rows):

    if rows.empty:
        return

    text = (
        "🏛️ **Nouveaux avis CEESP détectés**\n\n"
    )

    for _, row in rows.iterrows():

        text += (
            f"💊 {row['nom commercial']}\n"
            f"• DCI : {row['dci']}\n"
            f"• Indication : {row['indication']}\n"
            f"• Date : {format_date(row['date'])}\n\n"
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
        f"Teams status: {response.status_code}"
    )


def main():

    print("Starting CEESP monitor")

    df = load_data()

    df["key"] = df.apply(
        make_key,
        axis=1
    )

    old_keys = set()

    if os.path.exists(HISTORY_FILE):

        try:

            old_df = pd.read_csv(HISTORY_FILE)

            if "key" in old_df.columns:

                old_keys = set(old_df["key"])

        except Exception:

            pass

    new_rows = df[
        ~df["key"].isin(old_keys)
    ]

    print(
        f"{len(new_rows)} new rows detected"
    )

    if not new_rows.empty:

        send_teams(new_rows)

    df.to_csv(
        HISTORY_FILE,
        index=False
    )

    print("History updated")


if __name__ == "__main__":

    main()
