import logging
import os
import pathlib
import zipfile
from typing import Callable

import geopandas as gpd
import google.api_core.exceptions
import pandas as pd
import requests

from data_enricher import enrich_towns_with_area, enrich_roads_with_total_length, enrich_geovelo_with_length, \
    group_geovelo_by_insee_code, merge_all_dfs, enrich_postal_with_name, merge_geovelo_years
from storage import StorageClient


class DataLoader:
    DATA_DIR = pathlib.Path(__file__).parent / "data"
    STORAGE_PREFIX = "datasource/"
    FILES_INFOS = {
        "towns_population_2023": {
            "local_path": DATA_DIR / "towns_population_2023.zip",
            "storage_path": STORAGE_PREFIX + "towns_population_2023.zip",
            "download_url": "https://www.insee.fr/fr/statistiques/fichier/8680726/ensemble.zip"
        },
        "towns_politics_2020": {
            "local_path": DATA_DIR / "towns_politics_2020.csv",
            "storage_path": STORAGE_PREFIX + "towns_politics_2020.csv",
            "download_url": "https://www.data.gouv.fr/api/1/datasets/r/ea5d6bc3-37d0-4884-a437-155a90c3e05f"
        },
        "geovelo_2021_07": {
            "local_path": DATA_DIR / "geovelo_2021_07.geojson",
            "storage_path": STORAGE_PREFIX + "geovelo_2021_07.geojson",
            "download_url": "https://www.data.gouv.fr/api/1/datasets/r/3c478b0c-e8b5-48fc-a543-9739f2abb4dd"
        },
        "geovelo_2026_03": {
            "local_path": DATA_DIR / "geovelo_2026_03.geojson",
            "storage_path": STORAGE_PREFIX + "geovelo_2026_03.geojson",
            "download_url": "https://www.data.gouv.fr/api/1/datasets/r/142fecf6-4873-4dac-9876-2019d925eaf8"
        },
        "towns_geo": {
            "local_path": DATA_DIR / "towns_geo.geojson",
            "storage_path": STORAGE_PREFIX + "towns_geo.geojson",
            "download_url": "https://adresse.data.gouv.fr/data/contours-administratifs/latest/geojson/communes-5m.geojson"
        },
        "towns_roads_2026": {
            "local_path": DATA_DIR / "towns_roads_2026.csv",
            "storage_path": STORAGE_PREFIX + "towns_roads_2026.csv",
            "download_url": "https://www.data.gouv.fr/api/1/datasets/r/da40f3d7-d2e5-4668-ae42-2ad187f59265"
        },
        "postal_codes": {
            "local_path": DATA_DIR / "postal_codes.csv",
            "storage_path": STORAGE_PREFIX + "postal_codes.csv",
            "download_url": "https://www.data.gouv.fr/api/1/datasets/r/008a2dda-2c60-4b63-b910-998f6f818089"
        }
    }

    PLM_INFOS = {
        "Paris": {
            "insee": "75056",
            "arronds": ["75101", "75102", "75103", "75104", "75105", "75106", "75107", "75108", "75109",
                        "75110", "75111", "75112", "75113", "75114", "75115", "75116", "75117", "75118",
                        "75119", "75120"]
        },
        "Lyon": {
            "insee": "69123",
            "arronds": ["69381", "69382", "69383", "69384", "69385", "69386", "69387", "69388", "69389"]
        },
        "Marseille": {
            "insee": "13055",
            "arronds": ["13201", "13202", "13203", "13204", "13205", "13206", "13207", "13208", "13209",
                        "13210", "13211", "13212", "13213", "13214", "13215", "13216"]
        }
    }

    _raw_population_df = None
    _raw_politics_df = None
    _raw_roads_df = None
    _raw_postal_df = None
    _raw_geovelo_gdf_2021 = None
    _raw_geovelo_gdf_2026 = None
    _raw_towns_gdf = None
    _colors_df = None
    _processed_town_df = None
    _processed_roads_df = None
    _processed_postal_df = None
    _processed_geovelo_gdf_2021 = None
    _processed_geovelo_gdf_2026 = None
    _processed_unique_geovelo_gdf = None
    _processed_geovelo_length_df = None
    _merged_df = None

    @classmethod
    def get_raw_population_df(cls) -> pd.DataFrame:
        """
        Output columns: 'insee', 'nom', 'population'.
    
        In external file, Paris, Lyon and Marseille have one row by arrondissement, and no global row, so we merge it.
        """
        if cls._raw_population_df is None:
            # file is a zip, we have to unzip it first
            zip_path = cls._get_local_file_path("towns_population_2023")
            with zipfile.ZipFile(zip_path, "r") as zip_ref:
                zip_ref.extractall(cls.DATA_DIR)

            csv_path = cls.DATA_DIR / "donnees_communes.csv"
            types = {"COM": str, "Commune": str, "PMUN": int}
            population_df = pd.read_csv(csv_path, dtype=types, sep=";")

            columns_to_keep = ["COM", "Commune", "PMUN"]
            renaming = {"COM": "insee", "Commune": "nom", "PMUN": "population"}
            population_df = population_df[columns_to_keep].rename(columns=renaming)

            # merge PLM data into one row each
            new_rows = []
            for town_name, town_infos in cls.PLM_INFOS.items():
                mask = population_df["insee"].isin(town_infos["arronds"])
                new_rows.append({
                    "insee": town_infos["insee"],
                    "nom": town_name,
                    "population": population_df[mask]["population"].sum()
                })
            all_arronds = [arrond for town_infos in cls.PLM_INFOS.values() for arrond in town_infos["arronds"]]
            global_mask = ~population_df["insee"].isin(all_arronds)
            population_df = pd.concat([population_df[global_mask], pd.DataFrame(new_rows)], ignore_index=True)

            cls._raw_population_df = population_df
        return cls._raw_population_df

    @classmethod
    def get_raw_politics_df(cls) -> pd.DataFrame:
        """
        Output columns: 'insee', 'nuance_politique', 'famille_nuance'
    
        Paris, Lyon and Marseille have one row each, but Paris has no politics data on external file, so we add it.
        """
        if cls._raw_politics_df is None:
            types = {"cog_commune": str, "nuance_politique": str, "famille_nuance": str}
            politics_df = pd.read_csv(cls._get_local_file_path("towns_politics_2020"), dtype=types)

            columns_to_keep = ["cog_commune", "nuance_politique", "famille_nuance"]
            renaming = {"cog_commune": "insee"}
            politics_df = politics_df[columns_to_keep].rename(columns=renaming)

            # add Paris data (insee 75056) : nuance_politique = "LUG", famille_nuance = "Gauche"
            row_index = politics_df[politics_df["insee"] == "75056"].index[0]
            politics_df.at[row_index, "nuance_politique"] = "LUG"
            politics_df.at[row_index, "famille_nuance"] = "Gauche"
            cls._raw_politics_df = politics_df
        return cls._raw_politics_df

    @classmethod
    def get_raw_roads_df(cls) -> pd.DataFrame:
        """
        Output columns: 'insee', 'route_dept_principale_km', 'route_dept_secondaire_km', 'route_communale_km',
        'route_locale_km', 'rue_residentielle_km'
    
        Paris, Lyon and Marseille have one row each.
        """
        if cls._raw_roads_df is None:
            types = {"code_commune": str}
            roads_df = pd.read_csv(cls._get_local_file_path("towns_roads_2026"), dtype=types)

            columns_to_keep = ["code_commune",
                               "route_dept_principale_km", "route_dept_secondaire_km", "route_communale_km",
                               "route_locale_km", "rue_residentielle_km"]
            renaming = {"code_commune": "insee"}
            cls._raw_roads_df = roads_df[columns_to_keep].rename(columns=renaming)
        return cls._raw_roads_df

    @classmethod
    def get_raw_postal_df(cls) -> pd.DataFrame:
        """
        Output columns: 'insee', 'code_postal'

        Paris, Lyon and Marseille have several rows on original file, we redirect them to the main insee.
        """
        if cls._raw_postal_df is None:
            types = {"Code_commune_INSEE": str, "Code_postal": str}
            postal_df = pd.read_csv(cls._get_local_file_path("postal_codes"), dtype=types, sep=";", skiprows=1,
                                    names=("Code_commune_INSEE", "Nom_de_la_commune", "Code_postal",
                                           "Libellé_d_acheminement", "Ligne_5"),
                                    encoding="ISO-8859-1")

            columns_to_keep = ["Code_commune_INSEE", "Code_postal"]
            renaming = {"Code_commune_INSEE": "insee", "Code_postal": "code_postal"}
            postal_df = postal_df[columns_to_keep].rename(columns=renaming)

            # Handle PLM : convert arronds code to main insee
            for town_name, town_infos in cls.PLM_INFOS.items():
                mask = postal_df["insee"].isin(town_infos["arronds"])
                postal_df.loc[mask, "insee"] = town_infos["insee"]
            cls._raw_postal_df = postal_df
        return cls._raw_postal_df

    @classmethod
    def get_raw_geovelo_gdfs(cls) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
        """
        Output columns: 'insee_d', 'insee_g', 'geometry'

        For Paris, Lyon and Marseille, original files use insee codes of arrondissements, we use main code.
        """
        if cls._raw_geovelo_gdf_2021 is None or cls._raw_geovelo_gdf_2026 is None:
            geovelo_gdf_2021 = gpd.read_file(cls._get_local_file_path("geovelo_2021_07"))
            geovelo_gdf_2026 = gpd.read_file(cls._get_local_file_path("geovelo_2026_03"))

            columns_to_keep = ["code_com_d", "code_com_g", "geometry"]
            renaming = {"code_com_d": "insee_d", "code_com_g": "insee_g"}
            geovelo_gdf_2021 = geovelo_gdf_2021[columns_to_keep].rename(columns=renaming)
            geovelo_gdf_2026 = geovelo_gdf_2026[columns_to_keep].rename(columns=renaming)

            # Handle PLM : convert arronds code to main insee
            for town_name, town_infos in cls.PLM_INFOS.items():
                for col_name in ("insee_d", "insee_g"):
                    mask = geovelo_gdf_2021[col_name].isin(town_infos["arronds"])
                    geovelo_gdf_2021.loc[mask, col_name] = town_infos["insee"]
                    mask = geovelo_gdf_2026[col_name].isin(town_infos["arronds"])
                    geovelo_gdf_2026.loc[mask, col_name] = town_infos["insee"]

            cls._raw_geovelo_gdf_2021 = geovelo_gdf_2021.to_crs(epsg=27562)
            cls._raw_geovelo_gdf_2026 = geovelo_gdf_2026.to_crs(epsg=27562)
        return cls._raw_geovelo_gdf_2021, cls._raw_geovelo_gdf_2026

    @classmethod
    def get_raw_towns_gdf(cls) -> gpd.GeoDataFrame:
        """
        Output columns: 'insee', 'geometry'

        Raw file has data for both full PLM townns and each arrondissement, we leave it as it is.
        """
        if cls._raw_towns_gdf is None:
            towns_gdf = gpd.read_file(cls._get_local_file_path("towns_geo"))
            columns_to_keep = ["code", "geometry"]
            renaming = {"code": "insee"}
            cls._raw_towns_gdf = towns_gdf[columns_to_keep].rename(columns=renaming).to_crs(epsg=27562)
        return cls._raw_towns_gdf

    @classmethod
    def get_colors_df(cls) -> pd.DataFrame:
        if cls._colors_df is None:
            nuances = ['LEXG', 'LCOM', 'LFI', 'LSOC', 'LRDG', 'LDVG', 'LUG', 'LVEC', 'LECO', 'LDIV',
                       'LREG', 'LGJ', 'LREM', 'LMDM', 'LUDI', 'LUC', 'LDVC', 'LLR', 'LUD', 'LDVD', 'LDLF',
                       'LRN', 'LEXD', 'LNC', 'NC', '']
            nuances_meaning = ['Extrême gauche', 'Parti communiste français', 'La France insoumise',
                               'Parti socialiste', 'Parti radical de gauche', 'Divers gauche',
                               'Union de la gauche', 'Europe Ecologie-Les Verts', 'Ecologiste', 'Divers',
                               'Régionaliste', 'Gilets jaunes', 'La République en marche', 'Modem',
                               'Union des Démocrates et Indépendants', 'Union du centre',
                               'Divers centre', 'Les Républicains', 'Union de la droite',
                               'Divers droite', 'Debout la France', 'Rassemblement National',
                               'Extrême droite', 'Non Communiqué', 'Non Communiqué', 'Inconnu']
            primary_colors = ["bb0000", "dd0000", "cc2443", "ff8080", "ffd1dc", "ffc0c0", "cc6666", "00c000",
                              "00c000", "eeeeee", "DCBFA3", "FFFF00", "ffeb00", "ff9900", "00FFFF", "FAC577",
                              "FAC577", "0066cc", "0000ff", "adc1fd", "0082C4", "0D378A", "404040", "dddddd",
                              "dddddd", "dddddd"]
            secondary_colors = ['#' + complementary_color(c) for c in primary_colors]
            primary_colors = ['#' + c for c in primary_colors]
            cls._colors_df = pd.DataFrame({
                "nuance_politique": nuances,
                "nuance_politique_complete": nuances_meaning,
                "couleur_principale": primary_colors,
                "couleur_secondaire": secondary_colors
            })
        return cls._colors_df

    @classmethod
    def get_processed_town_df(cls) -> pd.DataFrame:
        """
        Output columns: 'insee', 'superficie'
        """
        if cls._processed_town_df is None:
            cls._processed_town_df = pd.read_parquet(
                cls._get_local_file_path("towns_df", lambda: enrich_towns_with_area(cls.get_raw_towns_gdf()))
            )
        return cls._processed_town_df

    @classmethod
    def get_processed_roads_df(cls) -> pd.DataFrame:
        """
        Output columns: 'insee', 'longueur_route'
        """
        if cls._processed_roads_df is None:
            cls._processed_roads_df = pd.read_parquet(
                cls._get_local_file_path("roads_df",
                                         lambda: enrich_roads_with_total_length(cls.get_raw_roads_df()))
            )
        return cls._processed_roads_df

    @classmethod
    def get_processed_geovelo_gdfs(cls) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
        """
        Output columns: 'insee', 'geometry'
        """
        if cls._processed_geovelo_gdf_2021 is None or cls._processed_geovelo_gdf_2026 is None:
            geovelo_2021_gdf, geovelo_2026_gdf = cls.get_raw_geovelo_gdfs()
            cls._processed_geovelo_gdf_2021 = gpd.read_parquet(
                cls._get_local_file_path(
                    "geovelo_2021_gdf",
                    lambda: group_geovelo_by_insee_code(geovelo_2021_gdf))
            )
            cls._processed_geovelo_gdf_2026 = gpd.read_parquet(
                cls._get_local_file_path(
                    "geovelo_2026_gdf",
                    lambda: group_geovelo_by_insee_code(geovelo_2026_gdf))
            )
        return cls._processed_geovelo_gdf_2021, cls._processed_geovelo_gdf_2026

    @classmethod
    def get_processed_unique_geovelo_gdf(cls) -> gpd.GeoDataFrame:
        """
        Output columns: 'insee', 'only_2026_geometry', 'only_2021_geometry', 'common_geometry'
        """
        if cls._processed_unique_geovelo_gdf is None:
            processed_geovelo_gdf_2021, processed_geovelo_gdf_2026 = cls.get_processed_geovelo_gdfs()
            cls._processed_unique_geovelo_gdf = gpd.read_parquet(
                cls._get_local_file_path(
                    "geovelo_unique_gdf",
                    lambda: merge_geovelo_years(processed_geovelo_gdf_2021, processed_geovelo_gdf_2026))
            )
        return cls._processed_unique_geovelo_gdf

    @classmethod
    def get_processed_geovelo_length_df(cls) -> pd.DataFrame:
        """
        Output columns: 'insee', 'longueur_piste_2026', 'longueur_piste_2021'
        """
        if cls._processed_geovelo_length_df is None:
            cls._processed_geovelo_length_df = pd.read_parquet(
                cls._get_local_file_path(
                    "geovelo_length_df",
                    lambda: enrich_geovelo_with_length(cls.get_processed_unique_geovelo_gdf()))
            )
        return cls._processed_geovelo_length_df

    @classmethod
    def get_processed_postal_df(cls) -> pd.DataFrame:
        """
        Output columns: 'insee', 'code_postal', 'nom'
        """
        if cls._processed_postal_df is None:
            cls._processed_postal_df = pd.read_parquet(
                cls._get_local_file_path("postal_df",
                                         lambda: enrich_postal_with_name(cls.get_raw_postal_df(),
                                                                         cls.get_raw_population_df()))
            )
        return cls._processed_postal_df

    @classmethod
    def get_merged_df(cls) -> pd.DataFrame:
        """
        Output columns:
        'insee', 'superficie', 'nom', 'population', 'nuance_politique', 'famille_nuance', 'nuance_politique_complete',
        'longueur_piste_2021', 'longueur_piste_2026', 'couleur_principale', 'couleur_secondaire', 'longueur_route'
        'code_postal'
        """
        if cls._merged_df is None:
            cls._merged_df = pd.read_parquet(
                cls._get_local_file_path("merged_df",
                                         lambda: merge_all_dfs(cls.get_processed_town_df(),
                                                               cls.get_raw_population_df(),
                                                               cls.get_raw_politics_df(),
                                                               cls.get_processed_roads_df(),
                                                               cls.get_raw_postal_df(),
                                                               cls.get_colors_df(),
                                                               cls.get_processed_geovelo_length_df()))
            )
        return cls._merged_df

    @classmethod
    def _get_local_file_path(cls, name: str, obtention_fn: Callable[[], pd.DataFrame] = None) -> str:
        """
        Try to find it on local filesystem,
            otherwise download it from storage,
            finally from the internet.

        (Storage is useful for performance and stability reasons,
        but this will still work if you don't have one, as long as URLs are valid)
        """
        file_infos = cls.FILES_INFOS.get(name)
        if file_infos is None:
            local_path = cls.DATA_DIR / f"{name}.parquet"
            storage_path = cls.STORAGE_PREFIX + f"{name}.parquet"
            download_url = None
        else:
            local_path = file_infos["local_path"]
            storage_path = file_infos["storage_path"]
            download_url = file_infos.get("download_url")

        if local_path.exists():
            logging.info(f"File {name} found on local filesystem.")
            return local_path
        logging.info(f"File {name} not found on local filesystem, trying to download it from storage...")

        os.makedirs(cls.DATA_DIR, exist_ok=True)
        if cls._download_file_from_storage(storage_path, local_path):
            logging.info(f"File {name} loaded from GCP storage.")
            return local_path
        logging.info(f"File {name} not found on GCP storage, trying to compute it or to download it from internet...")

        if download_url is not None:
            cls._download_file_from_internet(download_url, local_path)
            logging.info(f"File {name} loaded from direct URL.")
        else:
            df = obtention_fn() # df may be a geodataframe or a dataframe
            df.to_parquet(local_path, index=False)
            logging.info(f"File {name} computed.")

        cls._upload_file_to_storage(local_path, storage_path)
        return local_path

    @staticmethod
    def _download_file_from_storage(storage_path: str, local_path: str) -> bool:
        """
        Return True if the file was downloaded.
        """
        return False
        try:
            StorageClient.download_file(storage_path, local_path)
        except google.api_core.exceptions.NotFound:
            # no file on storage, this is fine
            return False
        except Exception as e:
            logging.warning(f"Error while downloading file from storage: {type(e)} {e}")
            return False
        else:
            return True

    @staticmethod
    def _upload_file_to_storage(local_path: str, storage_path: str):
        return
        try:
            StorageClient.upload_file(storage_path, local_path)
        except Exception as e:
            logging.warning(f"Error while uploading file to storage: {type(e)} {e}")

    @staticmethod
    def _download_file_from_internet(url: str, local_path: str):
        response = requests.get(url)
        response.raise_for_status()
        with open(local_path, "wb") as f:
            f.write(response.content)

    @classmethod
    def erase_all_cache(cls, properties: bool = True, files: bool = True, storage: bool = True):
        """
        Useful just for better DX during tests.
        """
        if properties:
            logging.info("Erasing all cached properties...")
            cls._raw_population_df = None
            cls._raw_politics_df = None
            cls._raw_roads_df = None
            cls._raw_postal_df = None
            cls._raw_geovelo_gdf_2021 = None
            cls._raw_geovelo_gdf_2026 = None
            cls._raw_towns_gdf = None
            cls._colors_df = None
            cls._processed_town_df = None
            cls._processed_roads_df = None
            cls._processed_postal_df = None
            cls._processed_geovelo_gdf_2021 = None
            cls._processed_geovelo_gdf_2026 = None
            cls._processed_unique_geovelo_gdf = None
            cls._processed_geovelo_length_df = None
            cls._merged_df = None
            logging.info("Erased.")

        if files:
            logging.info("Erasing all cached files...")
            if os.path.exists(cls.DATA_DIR):
                for file in os.listdir(cls.DATA_DIR):
                    if (file.endswith(".csv")
                            or file.endswith(".zip")
                            or file.endswith(".geojson")
                            or file.endswith(".parquet")):
                        os.remove(os.path.join(cls.DATA_DIR, file))
            logging.info("Erased.")

        if storage:
            logging.info("Erasing all cached files on storage...")
            try:
                StorageClient.empty_files()
            except Exception as e:
                logging.warning(f"Error while emptying storage: {type(e)} {e}")
            logging.info("Erased.")


def complementary_color(my_hex):
    return ''.join(['%02X' % (255 - int(a, 16)) for a in (my_hex[0:2], my_hex[2:4], my_hex[4:6])])


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    # DataLoader.erase_all_cache(storage=False)
    DataLoader.get_merged_df()
    DataLoader.get_processed_postal_df()
