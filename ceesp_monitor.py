import os
import hashlib
from io import StringIO

import pandas as pd
import requests


# ============================================================
# CONFIGURATION
# ============================================================

# URL CSV du tableau Tableau Public HAS
TABLEAU_URL = (
    "https://public.tableau.com/views/"
    "Contributionpatient/Tableaudebord5.csv"
    "?:showVizHome=no"
)

# Fichier historique
HISTORY_FILE = "history.csv"

# Secret GitHub contenant l'URL du webhook Teams
TEAMS_WEBHOOK = os.environ.get("TEAMS_WEBHOOK")

# URL du tableau HAS
TABLEAU_PAGE = (
    "https://public.tableau.com/app/profile/has8400/"
    "viz/Contributionpatient/Tableaudebord5"
)


# ============================================================
# COLONNES UTILISÉES POUR IDENTIFIER UNE CONTRIBUTION
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
# VÉRIFICATION DES COLONNES NÉCESSAIRES
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
# PREMIER LANCEMENT
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
# NETTOYAGE DE L'HISTORIQUE
# ============================================================

previous = previous.fillna("")


# ============================================================
# SI L'HISTORIQUE N'A PAS D'IDENTIFIANT
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
# SUPPRESSION DES DOUBLONS DE L'HISTORIQUE
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

def send_teams_message(
    medication_name,
    dci,
    indication,
    link
):

    # --------------------------------------------------------
    # Sécurité : valeurs par défaut
    # --------------------------------------------------------

    if not medication_name:
        medication_name = dci or "Médicament non renseigné"

    if not dci:
        dci = "Non renseignée"

    if not indication:
        indication = "Non renseignée"

    if not link:
        link = TABLEAU_PAGE


    # --------------------------------------------------------
    # ADAPTIVE CARD TEAMS
    # --------------------------------------------------------

    payload = {
        "type": "message",
        "attachments": [
            {
                "contentType": "application/vnd.microsoft.card.adaptive",
                "contentUrl": None,
                "content": {
                    "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                    "type": "AdaptiveCard",
                    "version": "1.2",

                    "body": [

                        {
                            "type": "TextBlock",
                            "text": "🆕 Nouvelle contribution patient – CEESP / HAS",
                            "weight": "Bolder",
                            "size": "Large",
                            "wrap": True
                        },

                        {
                            "type": "TextBlock",
                            "text": medication_name,
                            "weight": "Bolder",
                            "size": "Medium",
                            "wrap": True
                        },

                        {
                            "type": "TextBlock",
                            "text": f"DCI : {dci}",
                            "wrap": True
                        },

                        {
                            "type": "TextBlock",
                            "text": f"Indication : {indication}",
                            "wrap": True
                        }

                    ],

                    "actions": [

                        {
                            "type": "Action.OpenUrl",
                            "title": "🔗 Voir l'avis HAS",
                            "url": link
                        }

                    ]
                }
            }
        ]
    }


    # --------------------------------------------------------
    # ENVOI HTTP
    # --------------------------------------------------------

    try:

        response = requests.post(
            TEAMS_WEBHOOK,
            json=payload,
            headers={
                "Content-Type": "application/json"
            },
            timeout=30
        )

        print(
            f"Réponse Teams : HTTP {response.status_code}"
        )

        print(
            f"Réponse Teams : {response.text}"
        )

        response.raise_for_status()

    except requests.RequestException as error:

        raise RuntimeError(
            f"Erreur lors de l'envoi du message Teams : {error}"
        )

    print(
        "Notification Teams envoyée avec succès."
    )


# ============================================================
# ENVOI DES NOUVELLES CONTRIBUTIONS
# ============================================================

if len(new_rows) == 0:

    print(
        "Aucune nouvelle contribution."
    )

else:

    for _, row in new_rows.iterrows():

        # ----------------------------------------------------
        # Récupération des informations
        # ----------------------------------------------------

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


        # ----------------------------------------------------
        # Nom du médicament
        # ----------------------------------------------------

        medication_name = commercial

        if not medication_name:

            medication_name = dci


        # ----------------------------------------------------
        # Logs
        # ----------------------------------------------------

        print("")
        print(
            "Nouvelle contribution détectée : "
            f"{medication_name}"
        )

        print(
            f"DCI : {dci}"
        )

        print(
            f"Indication : {indication}"
        )

        print(
            f"Lien : {link}"
        )

        print(
            "Envoi de la notification Teams..."
        )


        # ----------------------------------------------------
        # ENVOI TEAMS
        # ----------------------------------------------------

        send_teams_message(
            medication_name=medication_name,
            dci=dci,
            indication=indication,
            link=link
        )


# ============================================================
# MISE À JOUR DE L'HISTORIQUE
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


# Suppression des éventuels doublons
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

print(
    "=============================================="
)
