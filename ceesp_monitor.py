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


def extract_visible_lines(driver):

    body_text = driver.find_element(
        "tag name",
        "body"
    ).text

    raw_lines = [

        line.strip()

        for line in body_text.split("\n")

        if line.strip()
    ]

    excluded = [

        "nom co",
        "dénomination",
        "indication courte",
        "validation",
        "pathologie",
        "view on tableau public",
        "share"

    ]

    cleaned = []

    for line in raw_lines:

        lower = line.lower()

        if any(
            x in lower
            for x in excluded
        ):

            continue

        cleaned.append(line)

    return cleaned


def rebuild_rows(lines):

    date_lines = [

        x for x in lines
        if is_date_line(x)
    ]

    n_rows = len(date_lines)

    if n_rows == 0:

        return []

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

    return rows


def scroll_tableau(driver):

    return driver.execute_script(
        """
        const els = Array.from(
            document.querySelectorAll('*')
        );

        let best = null;
        let bestHeight = 0;

        for (const el of els) {

            const style =
                window.getComputedStyle(el);

            const overflowY =
                style.overflowY;

            const scrollable =
                (
                    overflowY === 'auto'
                    || overflowY === 'scroll'
                );

            const height =
                el.scrollHeight;

            if (
                scrollable &&
                height > bestHeight &&
                height > 3000
            ) {

                best = el;
                bestHeight = height;
            }
        }

        if (!best)
            return -1;

        const before =
            best.scrollTop;

        best.scrollTop += 500;

        return {
            before: before,
            after: best.scrollTop,
            max: best.scrollHeight
        };
        """
    )


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
        "--window-size=1920,8000"
    )

    driver = webdriver.Chrome(
        options=chrome_options
    )

    print("Opening Tableau dashboard...")

    driver.get(TABLEAU_URL)

    time.sleep(25)

    all_rows = []

    seen = set()

    stuck_count = 0

    previous_scroll = -1

    print(
        "Scrolling through full Tableau history..."
    )

    for i in range(500):

        lines = extract_visible_lines(
            driver
        )

        rows = rebuild_rows(lines)

        for row in rows:

            key = (
                row["nom commercial"]
                + "|"
                + row["dci"]
                + "|"
                + row["indication"]
            )

            if key not in seen:

                seen.add(key)

                all_rows.append(row)

        print(
            f"Iteration {i} | "
            f"{len(all_rows)} rows collected"
        )

        scroll_state = scroll_tableau(
            driver
        )

        if scroll_state == -1:

            print(
                "No scroll container found"
            )

            break

        current_scroll = scroll_state[
            "after"
        ]

        print(
            f"Scroll position: "
            f"{current_scroll}"
        )

        if current_scroll == previous_scroll:

            stuck_count += 1

        else:

            stuck_count = 0

        previous_scroll = current_scroll

        if stuck_count >= 15:

            print(
                "Reached end of Tableau table"
            )

            break

        time.sleep(1.5)

    driver.quit()

    if not all_rows:

        raise Exception(
            "No rows reconstructed"
        )

    # final deduplication

    unique_rows = []

    final_seen = set()

    for row in all_rows:

        key = (
            row["nom commercial"]
            + "|"
            + row["dci"]
            + "|"
            + row["indication"]
        )

        if key not in final_seen:

            final_seen.add(key)

            unique_rows.append(row)

    df = pd.DataFrame(unique_rows)

    df.columns = [
        normalize_col(c)
        for c in df.columns
    ]

    print(
        f"{len(df)} final rows loaded"
    )

    print(df.tail(30))

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
            "🏛️ Nouveaux avis CEESP détectés\n\n"
            f"{count} nouveaux avis publiés\n\n"
        )
    else:
        text = (
            "🏛️ Nouvel avis CEESP détecté\n\n"
            "1 nouvel avis publié\n\n"
        )

    for _, row in rows.iterrows():

        nom = normalize_text(
            row[col_map["nom"]]
        ).upper()

        text += f"💊 {nom}\n"
        text += (
            f"DCI : "
            f"{normalize_text(row[col_map['dci']])}\n"
        )
        text += (
            f"Indication : "
            f"{normalize_text(row[col_map['indication']])}\n"
        )
        text += (
            f"Date de validation : "
            f"{format_date_fr(row[col_map['date']])}\n\n"
        )

    payload = {
        "text": text
    }

    print("===== TEAMS DEBUG =====")
    print(f"Webhook prefix: {TEAMS_WEBHOOK[:80]}")
    print("Payload:")
    print(payload)

    response = requests.post(
        TEAMS_WEBHOOK,
        json=payload,
        timeout=30
    )

    print(
        f"Teams status code: "
        f"{response.status_code}"
    )

    print(
        f"Teams response headers: "
        f"{dict(response.headers)}"
    )

    print(
        f"Teams response body: "
        f"{repr(response.text)}"
    )

    response.raise_for_status()

    print(
        "Teams notification sent successfully"
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

        print(
            new_rows[
                [
                    col_map["nom"],
                    col_map["dci"],
                    col_map["date"]
                ]
            ]
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
