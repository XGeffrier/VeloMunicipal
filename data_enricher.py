import geopandas as gpd
import pandas as pd


def enrich_towns_with_area(towns_gpd: gpd.GeoDataFrame) -> pd.DataFrame:
    """
    Add superficie info to a towns gpd. Remove geometry info to turn GeoDataFrame into DataFrame.

    Input columns: 'insee', 'geometry'
    Output columns: 'insee', 'superficie'
    """
    projected_towns_gpd = towns_gpd.to_crs(epsg=27562)
    projected_towns_gpd["superficie"] = projected_towns_gpd["geometry"].area

    towns_with_area_df = pd.DataFrame(projected_towns_gpd.drop(columns='geometry'))
    return towns_with_area_df


def enrich_geovelo_with_length(geovelo_gpd: gpd.GeoDataFrame) -> pd.DataFrame:
    """
    Add length info to a geovelo gpd. Remove geometry info to turn GeoDataFrame into DataFrame.

    Input columns: 'insee_d', 'insee_g', 'geometry'
    Output columns: 'insee_d', 'insee_g', 'longueur_piste'
    """
    return pd.DataFrame({"longueur_piste": geovelo_gpd.to_crs(epsg=27562)["geometry"].length,
                         "insee_d": geovelo_gpd["insee_d"],
                         "insee_g": geovelo_gpd["insee_g"]})


def group_geovelo_by_insee_code(geovelo_df: pd.DataFrame) -> pd.DataFrame:
    """
    Group geovelo by insee code.

    Input columns: 'insee_d', 'insee_g', 'longueur_piste'
    Output columns: 'insee', 'longueur_piste'
    """
    geovelo_df = (pd.concat([
        geovelo_df[["insee_g", "longueur_piste"]].rename(columns={"insee_g": "insee"}),
        geovelo_df[["insee_d", "longueur_piste"]].rename(columns={"insee_d": "insee"})
    ], ignore_index=True)
                  .groupby("insee", as_index=False)["longueur_piste"]
                  .sum())
    geovelo_df["longueur_piste"] = geovelo_df["longueur_piste"] / 2
    return geovelo_df


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
                  geovelo_2021_df: pd.DataFrame,
                  geovelo_2026_df: pd.DataFrame) -> pd.DataFrame:
    """
    Merged non-geometrical informations for each of the top n towns. Use local file system as cache.

    Output columns:
    'insee', 'superficie', 'nom', 'population', 'nuance_politique', 'famille_nuance', 'nuance_politique_complete',
    'longueur_piste_2021', 'longueur_piste_2026', 'couleur_principale', 'couleur_secondaire', 'longueur_route',
    'code_postal'
    """
    # merge dataframes on insee code (only keep codes that are in all datasets)
    merged_df = towns_df.merge(population_df, on="insee", how="inner")
    merged_df = merged_df.merge(politics_df, on="insee", how="inner")
    merged_df = merged_df.merge(roads_df, on="insee", how="inner")
    merged_df = merged_df.merge(postal_df, on="insee", how="inner")
    merged_df = merged_df.merge(colors_df, on="nuance_politique", how="inner")
    merged_df = (merged_df.merge(geovelo_2021_df, on="insee", how="left")
                 .rename(columns={"longueur_piste": "longueur_piste_2021"}))
    merged_df = (merged_df.merge(geovelo_2026_df, on="insee", how="left")
                 .rename(columns={"longueur_piste": "longueur_piste_2026"}))
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
