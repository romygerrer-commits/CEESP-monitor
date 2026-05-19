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


def setup_driver():

    options = Options()

    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,12000")

    driver = webdriver.Chrome(options=options)

    return driver


def scroll_table(driver):

    driver.execute_script("""

        const els = Array.from(document.querySelectorAll("*"));

        let target = null;
        let maxScroll = 0;

        for (const el of els) {

            if (el.scrollHeight > el.clientHeight) {

                if (el.scrollHeight > maxScroll) {

                    maxScroll = el.scrollHeight;
                    target = el;
                }
            }
        }

        if (target) {

            target.scrollTop += 800;
        }

    """)


def extract_all_cells(driver):

    collected = []

    seen = set()

    stable = 0
    previous_len = 0

    for i in range(200):

        time.sleep(2)

        cells = driver.find_elements(
            By.CSS_SELECTOR,
            "div[role='gridcell']"
        )

        for cell in cells:

            txt = normalize_text(cell.text)

            if txt == "":
                continue

            if txt in seen:
                continue

            seen.add(txt)

            collected.append(txt)

        print(f"Iteration {i} | {len(collected)} cells")

        if len(collected) == previous_len:

            stable += 1

        else:

            stable = 0

        previous_len = len(collected)

        if stable >= 10:

            print("End reached")
            break

        scroll_table(driver)

    return collected


def rebuild_rows(cells):

    rows = []

    i = 0

    while i + 3 < len(cells):

        nom = cells[i]
        dci = cells[i + 1]
        indication = cells[i + 2]
        date = cells[i + 3]

        rows.append({

            "nom commercial": nom,
            "dci": dci,
            "indication": indication,
            "date": date,
            "lien": ""

        })

        i += 4

    return rows


def load_data():

    print("Launching Chrome...")

    driver = setup_driver()

    print("Opening Tableau dashboard...")

    driver.get(TABLEAU_URL)

    time.sleep(20)

    print("Scrolling through Tableau...")

    cells = extract_all_cells(driver)

    driver.quit()

    print(f"{len(cells)} cells extracted")

    if len(cells) == 0:

        raise Exception("No Tableau cells extracted")

    print("Rebuilding rows...")

    rows = rebuild_rows(cells)

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
            f"• Date : {row['date']}\n\n"
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
        f"Teams notification: {response.status_code}"
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

    print(f"{len(new_rows)} new rows detected")

    if not new_rows.empty:

        send_teams(new_rows)

    df.to_csv(
        HISTORY_FILE,
        index=False
    )

    print("History updated")


if __name__ == "__main__":

    main()
