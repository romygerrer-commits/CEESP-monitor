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

# Nom du secret GitHub
TEAMS_WEBHOOK = os.environ.get("TEAMS_WEBHOOK")

TABLEAU_PAGE = (
    "https://public.tableau.com/app/profile/has8400/"
    "viz/Contributionpatient/Tableaudebord5"
)


# ============================================================
# COLONNES IMPORTANTES
# ============================================================

# Ces 4 informations servent à reconnaître une contribution
# de manière stable.

IDENTIFIER_COLUMNS = [
    "Dénomination Commune Internationale",
    "Indication courte (Pathologie?)",
    "Nom commercial",
    "Lien",
]


# ============================================================
# VÉRIFICATION DU WEBHOOK
# ============================================================

if not TEAMS_WEBHOOK:
    raise RuntimeError(
        "Le secret TEAMS_WEBHOOK n'est pas configuré dans GitHub."
    )


# ============================================================
# TÉLÉCHARGEMENT DU CSV TABLEAU
# ============================================================

print("Téléchargement du tableau HAS...")

try:

    response = requests.get(
        TABLEAU_URL,
        timeout=60,
        headers={
            "User-Agent": "Mozilla/5.0"
        }
    )

    response.raise_for_status()

except requests.RequestException as error:

    raise RuntimeError(
        f"Impossible de télécharger le tableau HAS : {error}"
    )


# ============================================================
# LECTURE DU CSV
# ============================================================

try:

    csv_content = response.content.decode("utf-8-sig")

    current = pd.read_csv(
        StringIO(csv_content),
        dtype=str
    )

except Exception as error:

    raise RuntimeError(
        f"Impossible de lire le CSV Tableau : {error}"
    )


print(
    f"Tableau téléchargé : "
    f"{len(current)} lignes / "
    f"{len(current.columns)} colonnes."
)


# ============================================================
# NETTOYAGE DES NOMS DE COLONNES
# ============================================================

current.columns = [
    str(column).strip()
    for column in current.columns
]


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
        "Colonnes nécessaires absentes du CSV Tableau : "
        + ", ".join(missing_columns)
    )


# ============================================================
# NETTOYAGE DES DONNÉES
# ============================================================

current = current.fillna("")

for column in current.columns:

    current[column] = (
        current[column]
        .astype(str)
        .str.strip()
    )


# ============================================================
# SUPPRESSION DES DOUBLONS
# ============================================================

before_duplicates = len(current)

current = current.drop_duplicates(
    subset=IDENTIFIER_COLUMNS,
    keep="first"
).copy()

after_duplicates = len(current)

duplicates_removed = (
    before_duplicates - after_duplicates
)

print(
    f"Doublons supprimés : {duplicates_removed}"
)

print(
    f"Contributions uniques : {after_duplicates}"
)


# ============================================================
# CRÉATION D'UN IDENTIFIANT UNIQUE
# ============================================================

def create_id(row):

    values = []

    for column in IDENTIFIER_COLUMNS:

        value = str(
            row[column]
        ).strip().lower()

        values.append(value)

    raw_value = "|||".join(values)

    return hashlib.sha256(
        raw_value.encode("utf-8")
    ).hexdigest()


current["_id"] = current.apply(
    create_id,
    axis=1
)


# ============================================================
# INITIALISATION DE L'HISTORIQUE
# ============================================================

if (
    not os.path.exists(HISTORY_FILE)
    or os.path.getsize(HISTORY_FILE) == 0
):

    print("")
    print("==============================================")
    print("PREMIER LANCEMENT")
    print("==============================================")

    print(
        f"{len(current)} contributions uniques trouvées."
    )

    print(
        "Création de l'historique initial..."
    )

    # On ne conserve que les informations utiles
    # dans history.csv.

    history_columns = [
        "_id",
        "Dénomination Commune Internationale",
        "Indication courte (Pathologie?)",
        "Nom commercial",
        "Lien",
    ]

    history = current[
        history_columns
    ].copy()

    history.to_csv(
        HISTORY_FILE,
        index=False,
        encoding="utf-8-sig"
    )

    print(
        "Historique initial créé."
    )

    print(
        "Aucune notification Teams envoyée."
    )

    print(
        "=============================================="
    )

    exit(0)


# ============================================================
# LECTURE DE L'HISTORIQUE
# ============================================================

try:

    previous = pd.read_csv(
        HISTORY_FILE,
        dtype=str,
        encoding="utf-8-sig"
    ).fillna("")

except pd.errors.EmptyDataError:

    print(
        "history.csv est vide."
    )

    print(
        "Initialisation de l'historique..."
    )

    history_columns = [
        "_id",
        "Dénomination Commune Internationale",
        "Indication courte (Pathologie?)",
        "Nom commercial",
        "Lien",
    ]

    current[
        history_columns
    ].to_csv(
        HISTORY_FILE,
        index=False,
        encoding="utf-8-sig"
    )

    print(
        "Aucune notification Teams envoyée."
    )

    exit(0)

except Exception as error:

    raise RuntimeError(
        f"Impossible de lire history.csv : {error}"
    )


# ============================================================
# NETTOYAGE DE L'ANCIEN HISTORIQUE
# ============================================================

previous = previous.fillna("")


# ============================================================
# SI L'ANCIEN HISTORY N'A PAS DE _id
# ============================================================

if "_id" not in previous.columns:

    print(
        "Création des identifiants dans l'ancien historique..."
    )

    missing_identifier_columns = [
        column
        for column in IDENTIFIER_COLUMNS
        if column not in previous.columns
    ]

    if missing_identifier_columns:

        raise RuntimeError(
            "history.csv ne contient pas les colonnes nécessaires : "
            + ", ".join(missing_identifier_columns)
        )

    previous["_id"] = previous.apply(
        create_id,
        axis=1
    )


# ============================================================
# SUPPRESSION DES DOUBLONS DANS L'HISTORIQUE
# ============================================================

previous = previous.drop_duplicates(
    subset="_id",
    keep="first"
).copy()


# ============================================================
# IDENTIFIANTS DÉJÀ CONNUS
# ============================================================

previous_ids = set(
    previous["_id"].astype(str)
)


# ============================================================
# DÉTECTION DES NOUVELLES CONTRIBUTIONS
# ============================================================

new_rows = current[
    ~current["_id"].isin(previous_ids)
].copy()


print("")
print("==============================================")
print("RÉSULTAT DU MONITORING")
print("==============================================")

print(
    f"Contributions actuelles : {len(current)}"
)

print(
    f"Contributions déjà connues : {len(previous_ids)}"
)

print(
    f"Nouvelles contributions : {len(new_rows)}"
)


# ============================================================
# FONCTION D'ENVOI TEAMS
# ============================================================

def send_teams_message(message):

    payload = {
        "text": message
    }

    try:

        response = requests.post(
            TEAMS_WEBHOOK,
            json=payload,
            timeout=30
        )

        response.raise_for_status()

    except requests.RequestException as error:

        raise RuntimeError(
            f"Erreur lors de l'envoi du message Teams : {error}"
        )

    print(
        "Notification Teams envoyée."
    )


# ============================================================
# ENVOI DES NOUVELLES CONTRIBUTIONS
# ============================================================

for _, row in new_rows.iterrows():

    # --------------------------------------------------------
    # Informations principales
    # --------------------------------------------------------

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

    link = row.get(
        "Lien",
        ""
    )


    # --------------------------------------------------------
    # Nom du médicament
    # --------------------------------------------------------

    medication_name = commercial

    if not medication_name:

        medication_name = dci


    # --------------------------------------------------------
    # Certaines colonnes peuvent être présentes dans Tableau
    # --------------------------------------------------------

    motif = ""

    if "Motif d'évaluation" in row.index:

        motif = row[
            "Motif d'évaluation"
        ]

    elif "Motif d'évaluation.1" in row.index:

        motif = row[
            "Motif d'évaluation.1"
        ]


    validation_date = ""

    if "Validation (date)" in row.index:

        validation_date = row[
            "Validation (date)"
        ]

    elif "Validation (date).1" in row.index:

        validation_date = row[
            "Validation (date).1"
        ]


    # --------------------------------------------------------
    # Message Teams
    # --------------------------------------------------------

    message = (
        "🆕 **Nouvelle contribution patient – CEESP / HAS**\n\n"

        f"💊 **Médicament :** {medication_name}\n\n"

        f"**DCI :** {dci}\n\n"

        f"🩺 **Indication :**\n"
        f"{indication}\n\n"

        f"📋 **Motif d'évaluation :**\n"
        f"{motif}\n\n"

        f"📅 **Date de validation :** "
        f"{validation_date}\n\n"

        f"🔗 **Lien HAS :**\n"
        f"{link}\n\n"

        f"📊 **Tableau des contributions patients :**\n"
        f"{TABLEAU_PAGE}"
    )


    print("")
    print(
        f"Nouvelle contribution détectée : "
        f"{medication_name}"
    )

    send_teams_message(
        message
    )


# ============================================================
# MISE À JOUR DE HISTORY.CSV
# ============================================================

history_columns = [
    "_id",
    "Dénomination Commune Internationale",
    "Indication courte (Pathologie?)",
    "Nom commercial",
    "Lien",
]


history = current[
    history_columns
].copy()


# Suppression de doublons éventuels
history = history.drop_duplicates(
    subset="_id",
    keep="first"
)


history.to_csv(
    HISTORY_FILE,
    index=False,
    encoding="utf-8-sig"
)


# ============================================================
# FIN
# ============================================================

print("")
print("==============================================")
print("MONITORING TERMINÉ")
print("==============================================")

print(
    f"Historique sauvegardé : {len(history)} entrées."
)

print(
    f"Notifications envoyées : {len(new_rows)}"
)

print("==============================================")
