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

# Fichier contenant les données déjà connues
HISTORY_FILE = "history.csv"

# Secret GitHub contenant l'URL du webhook Teams
TEAMS_WEBHOOK = os.environ.get("TEAMS_WEBHOOK")

# URL du tableau Tableau Public
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
# TÉLÉCHARGEMENT DU TABLEAU TABLEAU PUBLIC
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

except requests.RequestException as e:

    raise RuntimeError(
        f"Impossible de télécharger le tableau Tableau Public : {e}"
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

except Exception as e:

    raise RuntimeError(
        f"Impossible de lire le CSV Tableau Public : {e}"
    )


print(
    f"Tableau téléchargé : "
    f"{len(current)} lignes / "
    f"{len(current.columns)} colonnes."
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
# VÉRIFICATION DES COLONNES
# ============================================================

missing_columns = [
    column
    for column in IDENTIFIER_COLUMNS
    if column not in current.columns
]

if missing_columns:

    raise RuntimeError(
        "Les colonnes suivantes sont absentes du CSV Tableau : "
        + ", ".join(missing_columns)
    )


# ============================================================
# CRÉATION D'UN IDENTIFIANT UNIQUE POUR CHAQUE LIGNE
# ============================================================

def create_id(row):

    values = []

    for column in IDENTIFIER_COLUMNS:

        value = str(
            row[column]
        ).strip().lower()

        values.append(value)

    # Création d'une chaîne unique
    raw_id = "|||".join(values)

    # Hash pour obtenir un identifiant stable
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

# Si history.csv n'existe pas ou est vide,
# on initialise l'historique sans envoyer de notification.

if (
    not os.path.exists(HISTORY_FILE)
    or os.path.getsize(HISTORY_FILE) == 0
):

    print("Premier lancement détecté.")

    print(
        f"{len(current)} contributions trouvées."
    )

    print(
        "Création de l'historique initial..."
    )

    current.to_csv(
        HISTORY_FILE,
        index=False,
        encoding="utf-8-sig"
    )

    print(
        "Historique initial créé."
    )

    print(
        "Aucune notification Teams envoyée "
        "lors du premier lancement."
    )

    print(
        "Monitoring terminé."
    )

    exit(0)


# ============================================================
# LECTURE DE L'HISTORIQUE
# ============================================================

try:

    previous = pd.read_csv(
        HISTORY_FILE,
        dtype=str
    ).fillna("")

except pd.errors.EmptyDataError:

    print(
        "Le fichier history.csv est vide."
    )

    print(
        "Initialisation de l'historique..."
    )

    current.to_csv(
        HISTORY_FILE,
        index=False,
        encoding="utf-8-sig"
    )

    print(
        "Aucune notification Teams envoyée."
    )

    exit(0)

except Exception as e:

    raise RuntimeError(
        f"Impossible de lire history.csv : {e}"
    )


# ============================================================
# CRÉATION DE L'IDENTIFIANT DANS L'HISTORIQUE
# ============================================================

# Si l'ancien history.csv ne contient pas encore "_id",
# on le recrée automatiquement.

if "_id" not in previous.columns:

    print(
        "Ancien historique sans identifiant détecté."
    )

    previous["_id"] = previous.apply(
        create_id,
        axis=1
    )


# ============================================================
# LISTE DES ENTRÉES DÉJÀ CONNUES
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


print(
    f"Nouvelles contributions détectées : "
    f"{len(new_rows)}"
)


# ============================================================
# FONCTION D'ENVOI DU MESSAGE TEAMS
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

    except requests.RequestException as e:

        raise RuntimeError(
            f"Impossible d'envoyer le message Teams : {e}"
        )

    print(
        "Message Teams envoyé avec succès."
    )


# ============================================================
# ENVOI DES NOTIFICATIONS
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


        # ----------------------------------------------------
        # Si le nom commercial est vide,
        # on utilise la DCI
        # ----------------------------------------------------

        medication_name = commercial

        if not medication_name:

            medication_name = dci


        # ----------------------------------------------------
        # Construction du message Teams
        # ----------------------------------------------------

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


        # ----------------------------------------------------
        # Affichage dans les logs
        # ----------------------------------------------------

        print(
            "--------------------------------------------------"
        )

        print(
            f"Nouvelle contribution : {medication_name}"
        )

        print(
            f"DCI : {dci}"
        )

        print(
            f"Indication : {indication}"
        )

        print(
            f"Date : {validation_date}"
        )

        print(
            "Envoi de la notification Teams..."
        )


        # ----------------------------------------------------
        # Envoi Teams
        # ----------------------------------------------------

        send_teams_message(
            message
        )


# ============================================================
# MISE À JOUR DE L'HISTORIQUE
# ============================================================

current.to_csv(
    HISTORY_FILE,
    index=False,
    encoding="utf-8-sig"
)


print(
    "--------------------------------------------------"
)

print(
    "Historique mis à jour."
)

print(
    f"Nombre total d'entrées enregistrées : "
    f"{len(current)}"
)

print(
    "Monitoring terminé avec succès."
)
