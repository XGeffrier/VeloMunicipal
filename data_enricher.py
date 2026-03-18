import os

import geopandas as gpd
import pandas as pd

from data_loader import DATA_DIR

CACHE_DIR = DATA_DIR / "cache"


def enrich_towns_with_area(towns_gpd: gpd.GeoDataFrame) -> pd.DataFrame:
    """
    Add area info to a towns gpd. Remove geometry info to turn GeoDataFrame into DataFrame.

    Input columns: 'insee', 'geometry'
    Output columns: 'insee', 'area'
    """
    projected_towns_gpd = towns_gpd.to_crs(epsg=27562)
    projected_towns_gpd["area"] = projected_towns_gpd["geometry"].area

    towns_with_area_df = pd.DataFrame(projected_towns_gpd.drop(columns='geometry'))
    return towns_with_area_df


def enrich_geovelo_with_length(geovelo_gpd: gpd.GeoDataFrame) -> pd.DataFrame:
    """
    Add length info to a geovelo gpd. Remove geometry info to turn GeoDataFrame into DataFrame.

    Input columns: 'insee_d', 'insee_g', 'geometry'
    Output columns: 'insee_d', 'insee_g', 'length'
    """
    return pd.DataFrame({"length": geovelo_gpd.to_crs(epsg=27562)["geometry"].length,
                         "insee_d": geovelo_gpd["insee_d"],
                         "insee_g": geovelo_gpd["insee_g"]})


def group_geovelo_by_insee_code(geovelo_df: pd.DataFrame) -> pd.DataFrame:
    """
    Group geovelo by insee code.

    Input columns: 'insee_d', 'insee_g', 'length'
    Output columns: 'insee', 'bike_lane_length'
    """
    return (
        pd.concat([
            geovelo_df[["insee_g", "length"]].rename(columns={"insee_g": "insee", "length": "bike_lane_length"}),
            geovelo_df[["insee_d", "length"]].rename(columns={"insee_d": "insee", "length": "bike_lane_length"})
        ], ignore_index=True)
        .groupby("insee", as_index=False)["bike_lane_length"]
        .sum()
    )


def enrich_roads_with_total_length(roads_df: pd.DataFrame) -> pd.DataFrame:
    """
    Merge all roads length into one single column.

    Input columns: 'insee', 'route_dept_principale_km', 'route_dept_secondaire_km', 'route_communale_km',
    'route_locale_km', 'rue_residentielle_km'
    Output columns: 'insee', 'road_length'
    """
    return pd.DataFrame({"road_length": roads_df["route_dept_principale_km"] +
                                        roads_df["route_dept_secondaire_km"] +
                                        roads_df["route_communale_km"] +
                                        roads_df["route_locale_km"] +
                                        roads_df["rue_residentielle_km"],
                         "insee": roads_df["insee"]})


def merge_top_n_towns(towns_df: pd.DataFrame,
                      population_df: pd.DataFrame,
                      politics_df: pd.DataFrame,
                      roads_df: pd.DataFrame,
                      geovelo_2021_df: pd.DataFrame,
                      geovelo_2026_df: pd.DataFrame,
                      n: int = None) -> pd.DataFrame:
    """
    Merged non-geometrical informations for each of the top n towns. Use local file system as cache.

    Output columns:
    'insee', 'area', 'nom', 'population', 'nuance_politique', 'famille_nuance', 'road_length',
    'bike_lane_length_2021', 'bike_lane_length_2026'
    """
    if n is None:
        n = 35000
    # look for exact match in cache
    os.makedirs(CACHE_DIR, exist_ok=True)
    cache_filename = f"top_{n}_towns.csv"
    cache_filepath = os.path.join(CACHE_DIR, cache_filename)
    if os.path.exists(cache_filepath):
        return pd.read_csv(cache_filepath)

    # look for bigger match in cache
    for file in os.listdir(CACHE_DIR):
        if not (file.startswith("top_") and file.endswith(".csv")):
            continue
        file_n = int(file.split("_")[1])
        if file_n >= n:
            df = pd.read_csv(os.path.join(CACHE_DIR, file))
            df = df.sort_values(by="population", ascending=False)
            df = df.head(n)
            df.to_csv(cache_filepath, index=False)
            return df

    # no useful cache file found, create df
    population_df = population_df.sort_values(by="population", ascending=False)
    population_df = population_df.head(n)

    # merge dataframes on insee code (only keep codes that are in all datasets)
    merged_df = towns_df.merge(population_df, on="insee", how="inner")
    merged_df = merged_df.merge(politics_df, on="insee", how="inner")
    merged_df = merged_df.merge(roads_df, on="insee", how="inner")
    merged_df = (merged_df.merge(geovelo_2021_df, on="insee", how="left")
                 .rename(columns={"bike_lane_length": "bike_lane_length_2021"}))
    merged_df = (merged_df.merge(geovelo_2026_df, on="insee", how="left")
                 .rename(columns={"bike_lane_length": "bike_lane_length_2026"}))
    merged_df.to_csv(cache_filepath, index=False)
    return merged_df

#### DEPRECATED ####

def enrich_geovelo_gpd_with_town(geovelo_gpd: gpd.GeoDataFrame,
                                 towns_gpd: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    NOTE: this was done before acknowledging that town information is already in geovelo_gpd, so this function is not
    used anymore.

    Add town info to a geovelo gpd. Filter rows that are not in a town.
    """
    projected_geovelo_gpd = geovelo_gpd.to_crs(epsg=27562)
    projected_towns_gpd = towns_gpd.to_crs(epsg=27562)
    joined_gpd = projected_geovelo_gpd.sjoin_nearest(projected_towns_gpd, how="left", distance_col="distance_to_town")
    joined_gpd = joined_gpd[joined_gpd["distance_to_town"] == 0]
    return joined_gpd
