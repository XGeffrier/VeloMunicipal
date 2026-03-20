import logging

import geopandas as gpd
import pandas as pd
from shapely.geometry.point import Point


def enrich_towns_with_area(towns_gdf: gpd.GeoDataFrame) -> pd.DataFrame:
    """
    Add superficie info to a towns gpd. Remove geometry info to turn GeoDataFrame into DataFrame.

    Input columns: 'insee', 'geometry'
    Output columns: 'insee', 'superficie'
    """
    towns_gdf["superficie"] = towns_gdf["geometry"].area / 1_000_000

    towns_with_area_df = pd.DataFrame(towns_gdf.drop(columns='geometry'))
    return towns_with_area_df


def group_geovelo_by_insee_code(geovelo_single_year_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Input colomns : 'insee_d', 'insee_g', 'geometry'
    Output columns: 'insee', 'geometry'
    """
    geovelo_single_year_gdf = gpd.GeoDataFrame(pd.concat([
        geovelo_single_year_gdf[["insee_g", "geometry"]].rename(columns={"insee_g": "insee"}),
        geovelo_single_year_gdf[["insee_d", "geometry"]].rename(columns={"insee_d": "insee"})
    ], ignore_index=True))
    geovelo_single_year_gdf = geovelo_single_year_gdf.dissolve(by='insee', aggfunc='first')
    geovelo_single_year_gdf = geovelo_single_year_gdf.reset_index()
    return geovelo_single_year_gdf


def combine_geovelo_years(geovelo_gdf_2021: gpd.GeoDataFrame, geovelo_gdf_2026: gpd.GeoDataFrame) -> pd.DataFrame:
    """
    Input columns : 'insee', 'geometry' (2 dataframes, insee are unique)
    Output columns: 'insee', 'geometry' (3 dataframes)
    """
    merged = geovelo_gdf_2021.merge(geovelo_gdf_2026, on='insee', how='outer', suffixes=('_2021', '_2026'))

    tolerance = 20  # meters

    g21 = gpd.GeoSeries(merged['geometry_2021'], crs=geovelo_gdf_2021.crs)
    g26 = gpd.GeoSeries(merged['geometry_2026'], crs=geovelo_gdf_2026.crs)

    # Identify rows where one side is missing
    logging.info("Identify rows where one side is missing")
    null_21 = g21.is_empty | g21.isna()
    null_26 = g26.is_empty | g26.isna()
    both_present = ~null_21 & ~null_26
    g21_valid = g21.where(both_present, other=Point())
    g26_valid = g26.where(both_present, other=Point())

    logging.info("Buffer 1")
    buffered_21 = g21_valid.buffer(tolerance, resolution=1)
    logging.info("Buffer 2")
    buffered_26 = g26_valid.buffer(tolerance, resolution=1)

    logging.info("Difference 1")
    only_2026 = g26_valid.difference(buffered_21)
    logging.info("Difference 2")
    only_2021 = g21_valid.difference(buffered_26)
    logging.info("Difference 3")
    common = g21_valid.union(g26_valid).difference(only_2026).difference(only_2021)

    logging.info("Compute lengths")
    only_2026_length = only_2026.length / 1000
    only_2021_length = only_2021.length / 1000
    common_length = common.length / 1000

    # create geodataframe with 'insee' column and 'geometry' column for each of the 3 geometries
    # common_gdf = gpd.GeoDataFrame(pd.DataFrame({"insee": merged["insee"], "geometry": common}))
    # only_2026_gdf = gpd.GeoDataFrame(pd.DataFrame({"insee": merged["insee"], "geometry": only_2026}))
    # only_2021_gdf = gpd.GeoDataFrame(pd.DataFrame({"insee": merged["insee"], "geometry": only_2021}))

    logging.info("Return dataframe")
    return pd.DataFrame({"longueur_piste_2026": only_2026_length + common_length,
                         "longueur_piste_2021": only_2021_length + common_length,
                         "insee": merged["insee"]})


def enrich_roads_with_total_length(roads_df: pd.DataFrame) -> pd.DataFrame:
    """
    Merge all roads length into one single column.

    Input columns: 'insee', 'route_dept_principale_km', 'route_dept_secondaire_km', 'route_communale_km',
    'route_locale_km', 'rue_residentielle_km'
    Output columns: 'insee', 'longueur_route'
    """
    return pd.DataFrame({"longueur_route": roads_df["route_dept_principale_km"] +
                                           roads_df["route_dept_secondaire_km"] +
                                           roads_df["route_communale_km"] +
                                           roads_df["route_locale_km"] +
                                           roads_df["rue_residentielle_km"],
                         "insee": roads_df["insee"]})


def enrich_postal_with_name(postal_df: pd.DataFrame, population_df: pd.DataFrame) -> pd.DataFrame:
    """
    We can't use the name of raw postal file because it's ugly (no case, no diacritics).

    Output columns: 'insee', 'code_postal', 'nom'
    """
    merged_df = postal_df.merge(population_df, on="insee", how="inner")
    merged_df = merged_df[["insee", "code_postal", "nom"]]
    return merged_df


def merge_all_dfs(towns_df: pd.DataFrame,
                  population_df: pd.DataFrame,
                  politics_df: pd.DataFrame,
                  roads_df: pd.DataFrame,
                  postal_df: pd.DataFrame,
                  colors_df: pd.DataFrame,
                  geovelo_lengths_df: pd.DataFrame) -> pd.DataFrame:
    """
    Merged non-geometrical informations for each of the top n towns. Use local file system as cache.

    Output columns:
    'insee', 'superficie', 'nom', 'population', 'nuance_politique', 'famille_nuance', 'nuance_politique_complete',
    'longueur_piste_2021', 'longueur_piste_2026', 'couleur_principale', 'couleur_secondaire', 'longueur_route',
    'code_postal'
    """
    # merge dataframes on insee code
    merged_df = postal_df.merge(politics_df, on="insee", how="left").fillna('')
    merged_df = merged_df.merge(colors_df, on="nuance_politique", how="inner")
    merged_df = merged_df.merge(geovelo_lengths_df, on="insee", how="left").fillna(0)
    merged_df = merged_df.merge(population_df, on="insee", how="left")
    merged_df = merged_df.merge(roads_df, on="insee", how="inner")
    merged_df = merged_df.merge(towns_df, on="insee", how="inner")
    return merged_df


#### DEPRECATED ####

def enrich_geovelo_gdf_with_town(geovelo_gdf: gpd.GeoDataFrame,
                                 towns_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    NOTE: this was done before acknowledging that town information is already in geovelo_gdf, so this function is not
    used anymore.

    Add town info to a geovelo gpd. Filter rows that are not in a town.
    """
    projected_geovelo_gdf = geovelo_gdf.to_crs(epsg=27562)
    projected_towns_gdf = towns_gdf.to_crs(epsg=27562)
    joined_gdf = projected_geovelo_gdf.sjoin_nearest(projected_towns_gdf, how="left", distance_col="distance_to_town")
    joined_gdf = joined_gdf[joined_gdf["distance_to_town"] == 0]
    return joined_gdf
