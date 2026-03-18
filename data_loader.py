import pathlib

import geopandas as gpd
import pandas as pd

DATA_DIR = pathlib.Path(__file__).parent / "data"
SMALL_DATA_DIR = pathlib.Path(__file__).parent / "data/small"

POPULATION_DATA_FILE = "towns_population_2023.csv"
POLITICS_DATA_FILE = "towns_politics_2020.csv"
GEOVELO_2021_DATA_FILE = "geovelo_2021_07.geojson"
GEOVELO_2026_DATA_FILE = "geovelo_2026_03.geojson"
TOWNS_GEO_DATA_FILE = "towns_geo.geojson"
TOWNS_ROAD_DATA_FILE = "towns_roads_2026.csv"
POSTAL_DATA_FILE = "postal_codes.csv"


def load_population_df() -> pd.DataFrame:
    """
    Output columns: 'insee', 'nom', 'population'.

    Paris, Lyon and Marseille have one row by arrondissement, and no global row.
    """
    types = {"COM": str, "Commune": str, "PMUN": int}
    population_df = pd.read_csv(DATA_DIR / POPULATION_DATA_FILE, dtype=types, sep=";")

    columns_to_keep = ["COM", "Commune", "PMUN"]
    renaming = {"COM": "insee", "Commune": "nom", "PMUN": "population"}
    population_df = population_df[columns_to_keep].rename(columns=renaming)
    return population_df


def load_politics_df() -> pd.DataFrame:
    """
    Output columns: 'insee', 'nuance_politique', 'famille_nuance'

    Paris, Lyon and Marseille have one row each, but Paris has no politics data.
    """
    types = {"cog_commune": str, "nuance_politique": "category", "famille_nuance": "category"}
    politics_df = pd.read_csv(DATA_DIR / POLITICS_DATA_FILE, dtype=types)

    columns_to_keep = ["cog_commune", "nuance_politique", "famille_nuance"]
    renaming = {"cog_commune": "insee"}
    politics_df = politics_df[columns_to_keep].rename(columns=renaming)
    return politics_df


def load_roads_df() -> pd.DataFrame:
    """
    Output columns: 'insee', 'route_dept_principale_km', 'route_dept_secondaire_km', 'route_communale_km',
    'route_locale_km', 'rue_residentielle_km'

    Paris, Lyon and Marseille have one row each.
    """
    types = {"code_commune": str}
    roads_df = pd.read_csv(DATA_DIR / TOWNS_ROAD_DATA_FILE, dtype=types)

    columns_to_keep = ["code_commune",
                       "route_dept_principale_km", "route_dept_secondaire_km", "route_communale_km", "route_locale_km",
                       "rue_residentielle_km"]
    renaming = {"code_commune": "insee"}
    roads_df = roads_df[columns_to_keep].rename(columns=renaming)
    return roads_df


def load_postal_df() -> pd.DataFrame:
    """
    Output columns: 'insee', 'code_postal', 'nom'
    """
    types = {"Code_commune_INSEE": str, "Nom_de_la_commune": str, "Code_postal": str}
    postal_df = pd.read_csv(DATA_DIR / POSTAL_DATA_FILE, dtype=types, sep=";")

    columns_to_keep = ["Code_commune_INSEE", "Code_postal", "Nom_de_la_commune"]
    renaming = {"Code_commune_INSEE": "insee", "Code_postal": "code_postal", "Nom_de_la_commune": "nom"}
    return postal_df[columns_to_keep].rename(columns=renaming)


def load_geovelo_gpd(small: bool = False) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
    """
    Output columns: 'insee_d', 'insee_g', 'geometry'
    """
    data_dir = SMALL_DATA_DIR if small else DATA_DIR
    paths = (data_dir / GEOVELO_2021_DATA_FILE, data_dir / GEOVELO_2026_DATA_FILE)
    geovelo_2021_gpd, geovelo_2026_gpd = gpd.read_file(paths[0]), gpd.read_file(paths[1])

    columns_to_keep = ["code_com_d", "code_com_g", "geometry"]
    renaming = {"code_com_d": "insee_d", "code_com_g": "insee_g"}
    geovelo_2021_gpd = geovelo_2021_gpd[columns_to_keep].rename(columns=renaming)
    geovelo_2026_gpd = geovelo_2026_gpd[columns_to_keep].rename(columns=renaming)
    return geovelo_2021_gpd, geovelo_2026_gpd


def load_towns_gpd(small: bool = False) -> gpd.GeoDataFrame:
    """
    Output columns: 'insee', 'geometry'
    """
    data_dir = SMALL_DATA_DIR if small else DATA_DIR
    towns_gpd = gpd.read_file(data_dir / TOWNS_GEO_DATA_FILE)
    columns_to_keep = ["code", "geometry"]
    renaming = {"code": "insee"}
    towns_gpd = towns_gpd[columns_to_keep].rename(columns=renaming)
    return towns_gpd

if __name__ == '__main__':
    postal_df = load_postal_df()