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
    'code_postal', 'nom', 'insee'
    """
    return [{"code_postal": row["code_postal"], "nom": row["nom"], "insee": row["insee"]}
            for _, row in DataLoader.get_processed_postal_df().iterrows()]


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


def get_similar_communes(insee_code: str, comparison_mode: str = 'famille') -> list[dict]:
    """
    Return up to 5 communes based on comparison mode.

    Args:
        insee_code: Target commune INSEE code
        comparison_mode: 'famille' (default) to compare by political family, 'size' to compare by population range (±40%)

    If comparison_mode is 'famille' and famille_nuance not available, falls back to population range.

    Returns list of dicts with keys:
    'insee', 'nom', 'code_postal', 'population', 'famille_nuance', 'longueur_piste_2021', 'longueur_piste_2026', 'couleur_principale'
    """
    main_df = DataLoader.get_merged_df()

    # Get target commune
    target = main_df[main_df['insee'] == insee_code]
    if target.empty:
        return []

    target_row = target.iloc[0]
    target_famille = target_row.get('famille_nuance')
    target_pop = target_row.get('population')

    # Filter candidates (exclude target)
    candidates = main_df[main_df['insee'] != insee_code].copy()

    # Filter based on comparison mode
    if comparison_mode == 'famille':
        # Compare by political family
        if is_valid(target_famille):
            filtered = candidates[candidates['famille_nuance'] == target_famille]
        else:
            # Fall back to population range (±40%)
            if is_valid(target_pop) and target_pop > 0:
                lower_bound = target_pop * 0.6
                upper_bound = target_pop * 1.4
                filtered = candidates[(candidates['population'] >= lower_bound) &
                                     (candidates['population'] <= upper_bound)]
            else:
                return []
    elif comparison_mode == 'size':
        # Compare by population range (±40%)
        if is_valid(target_pop) and target_pop > 0:
            lower_bound = target_pop * 0.6
            upper_bound = target_pop * 1.4
            filtered = candidates[(candidates['population'] >= lower_bound) &
                                 (candidates['population'] <= upper_bound)]
        else:
            return []
    else:
        return []

    if filtered.empty:
        return []

    # Remove duplicates by insee
    filtered = filtered.drop_duplicates(subset=['insee'], keep='first')

    # Sort by absolute population difference
    filtered['pop_diff'] = (filtered['population'] - target_pop).abs()
    filtered = filtered.sort_values('pop_diff')

    # Return top 5
    result = []
    for _, row in filtered.head(5).iterrows():
        result.append({
            'insee': row['insee'],
            'nom': row['nom'],
            'code_postal': row['code_postal'],
            'population': row['population'],
            'famille_nuance': row['famille_nuance'],
            'longueur_piste_2021': row['longueur_piste_2021'],
            'longueur_piste_2026': row['longueur_piste_2026'],
            'couleur_principale': row['couleur_principale'],
        })

    return result


def is_valid(value):
    if value is None:
        return False
    try:
        return not pd.isna(value)
    except:
        return True
