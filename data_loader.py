import os
import pathlib

import geopandas as gpd
import pandas as pd

DATA_DIR = pathlib.Path(__file__).parent / "data"
STORAGE_PREFIX = ""
FILES_INFOS = [{"name": "towns_population_2023",
                "local_path": DATA_DIR / "towns_population_2023.csv",
                "storage_path": STORAGE_PREFIX + "towns_population_2023.csv",
                "download_url": ""},
               {"name": "towns_politics_2020",
                "local_path": DATA_DIR / "towns_politics_2020.csv",
                "storage_path": STORAGE_PREFIX + "towns_politics_2020.csv",
                "download_url": ""},
               {"name": "geovelo_2021_07",
                "local_path": DATA_DIR / "geovelo_2021_07.geojson",
                "storage_path": STORAGE_PREFIX + "geovelo_2021_07.geojson",
                "download_url": ""},
               {"name": "geovelo_2026_03",
                "local_path": DATA_DIR / "geovelo_2026_03.geojson",
                "storage_path": STORAGE_PREFIX + "geovelo_2026_03.geojson",
                "download_url": ""},
               {"name": "towns_geo",
                "local_path": DATA_DIR / "towns_geo.geojson",
                "storage_path": STORAGE_PREFIX + "towns_geo.geojson",
                "download_url": ""},
               {"name": "towns_roads_2026",
                "local_path": DATA_DIR / "towns_roads_2026.csv",
                "storage_path": STORAGE_PREFIX + "towns_roads_2026.csv",
                "download_url": ""},
               {"name": "postal_codes",
                "local_path": DATA_DIR / "postal_codes.csv",
                "storage_path": STORAGE_PREFIX + "postal_codes.csv",
                "download_url": ""}]


def load_population_df() -> pd.DataFrame:
    """
    Output columns: 'insee', 'nom', 'population'.

    Paris, Lyon and Marseille have one row by arrondissement, and no global row.
    """
    types = {"COM": str, "Commune": str, "PMUN": int}
    population_df = pd.read_csv(_get_local_file_path("towns_population_2023"), dtype=types, sep=";")

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
    politics_df = pd.read_csv(_get_local_file_path("towns_politics_2020"), dtype=types)

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
    roads_df = pd.read_csv(_get_local_file_path("towns_roads_2026"), dtype=types)

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
    postal_df = pd.read_csv(_get_local_file_path("postal_codes"), dtype=types, sep=";")

    columns_to_keep = ["Code_commune_INSEE", "Code_postal", "Nom_de_la_commune"]
    renaming = {"Code_commune_INSEE": "insee", "Code_postal": "code_postal", "Nom_de_la_commune": "nom"}
    return postal_df[columns_to_keep].rename(columns=renaming)


def load_geovelo_gpd() -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
    """
    Output columns: 'insee_d', 'insee_g', 'geometry'
    """
    geovelo_2021_gpd = gpd.read_file(_get_local_file_path("geovelo_2021_07"))
    geovelo_2026_gpd = gpd.read_file(_get_local_file_path("geovelo_2026_03"))

    columns_to_keep = ["code_com_d", "code_com_g", "geometry"]
    renaming = {"code_com_d": "insee_d", "code_com_g": "insee_g"}
    geovelo_2021_gpd = geovelo_2021_gpd[columns_to_keep].rename(columns=renaming)
    geovelo_2026_gpd = geovelo_2026_gpd[columns_to_keep].rename(columns=renaming)
    return geovelo_2021_gpd, geovelo_2026_gpd


def load_towns_gpd() -> gpd.GeoDataFrame:
    """
    Output columns: 'insee', 'geometry'
    """
    towns_gpd = gpd.read_file(_get_local_file_path("towns_geo"))
    columns_to_keep = ["code", "geometry"]
    renaming = {"code": "insee"}
    towns_gpd = towns_gpd[columns_to_keep].rename(columns=renaming)
    return towns_gpd


def _get_local_file_path(name: str) -> str:
    """
    Try to find it on local filesystem,
    otherwise download it from storage,
    finally from the internet.
    """
    file_infos = next(filter(lambda x: x["name"] == name, FILES_INFOS))
    if file_infos["local_path"].exists():
        return file_infos["local_path"]
    os.makedirs(DATA_DIR, exist_ok=True)
    if _download_file_from_storage(file_infos["storage_path"], file_infos["local_path"]):
        return file_infos["storage_path"]
    _download_file_from_internet(file_infos["download_url"], file_infos["local_path"])
    _upload_file_to_storage(file_infos["local_path"], file_infos["storage_path"])
    return file_infos["local_path"]


def _download_file_from_storage(storage_path: str, local_path: str) -> bool:
    raise NotImplementedError


def _upload_file_to_storage(local_path: str, storage_path: str):
    raise NotImplementedError


def _download_file_from_internet(url: str, local_path: str):
    raise NotImplementedError


if __name__ == '__main__':
    postal_df = load_postal_df()
