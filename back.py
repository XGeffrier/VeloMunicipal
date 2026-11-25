"""
This file is the only one imported by Flask app. It gives high-level access to the data.
"""
import functools

import pandas as pd

from data_loader import DataLoader


def load_on_startup():
    """
    Load once the useful dataframes. Call that on instance startup to speed up the first requests.
    """
    DataLoader.get_processed_postal_df()
    DataLoader.get_merged_df()
    get_all_communes()

@functools.cache
def get_all_communes() -> list[dict]:
    """
    Return a list of communes, each commune being a dict with the following keys:
    'code_postal', 'nom', 'insee', 'population'
    """
    postal_df = DataLoader.get_processed_postal_df().merge(
        DataLoader.get_merged_df()[['insee', 'population']],
        on='insee',
        how='left'
    )
    return [{"code_postal": row["code_postal"], "nom": row["nom"], "insee": row["insee"],
             "population": int(row["population"]) if not pd.isna(row["population"]) else 0}
            for _, row in postal_df.iterrows()]


def get_data_of(insee_code: str) -> dict | None:
    """
    Output dict keys:
    'insee', 'superficie', 'nom', 'population', 'nuance_politique', 'famille_nuance', 'nuance_politique_complete',
    'longueur_piste_2021', 'longueur_piste_2026', 'couleur_principale', 'couleur_secondaire', 'longueur_route'
    """
    main_df = DataLoader.get_merged_df()
    try:
        row = main_df[main_df['insee'] == insee_code].iloc[0]
    except IndexError:
        return None
    return row.to_dict()


def is_valid(value):
    if value is None:
        return False
    try:
        return not pd.isna(value)
    except:
        return True
