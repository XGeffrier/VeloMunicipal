from data_loader import DataLoader

def get_all_communes() -> list[dict]:
    """
    Return a list of communes, each commune being a dict with the following keys:
    'zipCode', 'name', 'inseeCode'
    """
    return [{"zipCode": row["code_postal"], "name": row["nom"], "inseeCode": row["insee"]}
            for _, row in DataLoader.get_processed_postal_df().iterrows()]


def get_data_of(insee_code: str) -> dict:
    """
    Output dict keys:
    'insee', 'superficie', 'nom', 'population', 'nuance_politique', 'famille_nuance', 'nuance_politique_complete',
    'longueur_piste_2021', 'longueur_piste_2026', 'couleur_principale', 'couleur_secondaire', 'longueur_route'
    """
    main_df = DataLoader.get_merged_df()
    row = main_df[main_df['insee'] == insee_code].iloc[0]
    return row.to_dict()