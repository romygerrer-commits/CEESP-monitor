import os
import hashlib
from io import StringIO

import pandas as pd
import requests


# ============================================================
# CONFIGURATION
# ============================================================

TABLEAU_URL = (
    "https://public.tableau.com/views/"
    "Contributionpatient/Tableaudebord5.csv"
    "?:showVizHome=no"
)

HISTORY_FILE = "history.csv"

TEAMS_WEBHOOK_URL = os.environ.get("TEAMS_WEBHOOK_URL")

TABLEAU_PAGE = (
    "https://public.tableau.com/app/profile/has8400/"
    "viz/Contributionpatient/Tableaudebord5"
)


# ============================================================
# COLONNES DU CSV UTILISÉES POUR IDENTIFIER UNE CONTRIBUTION
# ============================================================

IDENTIFIER_COLUMNS = [
    "Dénomination Commune Internationale",
    "Indication courte (Pathologie?)",
    "Nom commercial",
    "Lien",
]


# ============================================================
# VÉRIFICATION DU WEBHOOK
# ============================================================

if not TEAMS_WEBHOOK_URL:
    raise RuntimeError(
        "Le secret TEAMS_WEBHOOK_URL n'est pas configuré dans GitHub."
    )


# ============================================================
# TÉLÉCHARGEMENT DU CSV TABLEAU
# ============================================================

print("Téléchargement du tableau HAS...")

response = requests.get(
    TABLEAU_URL,
    timeout=60,
    headers={
        "User-Agent": "Mozilla/5.0"
    }
)

response.raise_for_status()

csv_content = response.content.decode("utf-8-sig")

current = pd.read_csv(
    StringIO(csv_content),
    dtype=str
).fillna("")


print(
    f"Tableau téléchargé : "
    f"{len(current)} lignes / "
    f"{len(current.columns)} colonnes."
)


# ============================================================
# NETTOYAGE
# ============================================================

for column in current.columns:
    current[column] = (
        current[column]
        .astype(str)
        .str.strip()
    )


# ============================================================
# VÉRIFICATION DES COLONNES
# ============================================================

missing_columns = [
    column
    for column in IDENTIFIER_COLUMNS
    if column not in current.columns
]

if missing_columns:
    raise RuntimeError(
        "Colonnes manquantes dans le CSV Tableau : "
        + ", ".join(missing_columns)
    )


# ============================================================
# IDENTIFIANT UNIQUE
# ============================================================

def create_id(row):
    """
    Crée un identifiant stable pour chaque contribution.
    """

    values = []

    for column in IDENTIFIER_COLUMNS:
        value = str(row[column]).strip().lower()
        values.append(value)

    raw_id = "|||".join(values)

    return hashlib.sha256(
        raw_id.encode("utf-8")
    ).hexdigest()


current["_id"] = current.apply(
    create_id,
    axis=1
)


# ============================================================
# PREMIER LANCEMENT
# ============================================================

if not os.path.exists(HISTORY_FILE):

    print(
        "Premier lancement détecté."
    )

    print(
        f"{len(current)} contributions enregistrées "
        "comme historique initial."
    )

    current.to_csv(
        HISTORY_FILE,
        index=False,
        encoding="utf-8-sig"
    )

    print(
        "Aucune notification Teams envoyée "
        "lors du premier lancement."
    )

    exit(0)


# ============================================================
# CHARGEMENT DE L'HISTORIQUE
# ============================================================

previous = pd.read_csv(
    HISTORY_FILE,
    dtype=str
).fillna("")


if "_id" not in previous.columns:

    print(
        "Ancien historique sans identifiant détecté."
    )

    previous["_id"] = previous.apply(
        create_id,
        axis=1
    )


previous_ids = set(
    previous["_id"]
)


# ============================================================
# DÉTECTION DES NOUVELLES CONTRIBUTIONS
# ============================================================

new_rows = current[
    ~current["_id"].isin(previous_ids)
].copy()


print(
    f"Nouvelles contributions détectées : "
    f"{len(new_rows)}"
)


# ============================================================
# ENVOI DU MESSAGE TEAMS
# ============================================================

def send_teams(message):

    payload = {
        "text": message
    }

    response = requests.post(
        TEAMS_WEBHOOK_URL,
        json=payload,
        timeout=30
    )

    response.raise_for_status()


# ============================================================
# FORMATAGE ET ENVOI DES NOTIFICATIONS
# ============================================================

for _, row in new_rows.iterrows():

    dci = row.get(
        "Dénomination Commune Internationale",
        ""
    )

    indication = row.get(
        "Indication courte (Pathologie?)",
        ""
    )

    commercial = row.get(
        "Nom commercial",
        ""
    )

    validation_date = row.get(
        "Validation (date)",
        ""
    )

    motif = row.get(
        "Motif d'évaluation",
        ""
    )

    link = row.get(
        "Lien",
        ""
    )


    message = (
        "🆕 **Nouvel avis CEESP / HAS**\n\n"
        f"💊 **Médicament :** {commercial}\n\n"
        f"**DCI :** {dci}\n\n"
        f"🩺 **Indication :** {indication}\n\n"
        f"📅 **Date de validation :** {validation_date}\n\n"
        f"🔗 **Lien HAS :** {link}\n\n"
        f"📊 **Tableau des contributions patients :** "
        f"{TABLEAU_PAGE}"
    )


    print(
        f"Envoi de la notification Teams : "
        f"{commercial}"
    )

    send_teams(message)


# ============================================================
# MISE À JOUR DE L'HISTORIQUE
# ============================================================

current.to_csv(
    HISTORY_FILE,
    index=False,
    encoding="utf-8-sig"
)


print(
    "Historique mis à jour."
)

print(
    "Monitoring terminé."
)
