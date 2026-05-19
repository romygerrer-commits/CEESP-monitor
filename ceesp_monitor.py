import os
import pandas as pd
import requests

from tableauscraper import TableauScraper as TS


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

            return date_obj.strftime("%d/%m/%Y")

    except Exception:

        pass

    return normalize_text(value)


def load_data():

    print("Loading Tableau dashboard...")

    ts = TS()

    ts.session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        )
    })

    try:

        ts.loads(TABLEAU_URL)

    except Exception as e:

        print(f"Initial Tableau load failed: {e}")

        print("Trying fallback URL...")

        fallback_url = (
            "https://public.tableau.com/views/"
            "Contributionpatient/Tableaudebord5"
            "?:language=fr-FR&:display_count=n"
            "&:origin=viz_share_link"
        )

        ts.loads(fallback_url)

    workbook = ts.getWorkbook()

    worksheet_names = workbook.getWorksheetNames()

    print("Available worksheets:")
    print(worksheet_names)

    target_ws = None

    for ws_name in worksheet_names:

        try:

            ws = workbook.getWorksheet(ws_name)

            df = ws.data

            if df.empty:
                continue

            cols = [
                normalize_col(c)
                for c in df.columns
            ]

            print(f"Worksheet {ws_name}:")
            print(cols)

            if any(
                "nom commercial" in c
                for c in cols
            ):

                target_ws = ws_name
                break

        except Exception as e:

            print(
                f"Error reading worksheet "
                f"{ws_name}: {e}"
            )

    if target_ws is None:

        raise Exception(
            "Could not find worksheet "
            "containing CEESP data"
        )

    print(f"Using worksheet: {target_ws}")

    ws = workbook.getWorksheet(target_ws)

    df = ws.data

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

            print("Columns available:")
            print(df.columns.tolist())

            raise Exception(
                f"Missing required column: {req}"
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
            "🏛️ **Nouveaux avis CEESP détectés**\n\n"
            f"{count} nouveaux avis publiés\n\n"
        )

    else:

        text = (
            "🏛️ **Nouvel avis CEESP détecté**\n\n"
            "1 nouvel avis publié\n\n"
        )

    for _, row in rows.iterrows():

        text += "\n\u200b\n"

        nom = normalize_text(
            row[col_map["nom"]]
        ).upper()

        if (
            "lien" in col_map
            and pd.notna(row[col_map["lien"]])
            and normalize_text(
                row[col_map["lien"]]
            ) != ""
        ):

            url = normalize_text(
                row[col_map["lien"]]
            )

            text += f"💊 [{nom}]({url})\n\n"

        else:

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

            date_fr = format_date_fr(
                row[col_map["date"]]
            )

            text += (
                f"• Date de validation : "
                f"{date_fr}\n\n"
            )

    text += (
        "\n🔎 Tableau de bord complet :\n"
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

        print("No new CEESP entries")

    df.to_csv(
        HISTORY_FILE,
        index=False
    )

    print("History updated")


if __name__ == "__main__":

    main()
