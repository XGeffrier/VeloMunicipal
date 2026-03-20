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


def merge_geovelo_years(geovelo_gdf_2021: gpd.GeoDataFrame, geovelo_gdf_2026: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Input colomns : 'insee', 'geometry' (2 dataframes, insee are unique)
    Output columns: 'insee', 'only_2026_geometry', 'only_2021_geometry', 'common_geometry' (1 dataframe)
    """
    merged = geovelo_gdf_2021.merge(geovelo_gdf_2026, on='insee', how='outer', suffixes=('_2021', '_2026'))

    # We have to use a tolerance: sometimes lanes are slightly different in 2021 and 2026, but are actually the same.
    tolerance = 20  # meters

    def compute_one_row(row):
        geom_2021 = row['geometry_2021']
        geom_2026 = row['geometry_2026']

        if geom_2021 is None or geom_2026 is None:
            row["only_2026_geometry"] = geom_2026 if geom_2026 is not None else Point()
            row["only_2021_geometry"] = geom_2021 if geom_2021 is not None else Point()
            row["common_geometry"] = Point()
            return row

        geom_2021_buffered = geom_2021.buffer(tolerance)
        geom_2026_buffered = geom_2026.buffer(tolerance)

        only_2026_geometry = geom_2026.difference(geom_2021_buffered)
        only_2021_geometry = geom_2021.difference(geom_2026_buffered)
        common_geometry = geom_2021.union(geom_2026).difference(only_2026_geometry).difference(only_2021_geometry)

        row["only_2026_geometry"] = only_2026_geometry
        row["only_2021_geometry"] = only_2021_geometry
        row["common_geometry"] = common_geometry

        return row

    merged = merged.apply(compute_one_row, axis=1)
    merged = merged.drop(columns=['geometry_2021', 'geometry_2026'])

    return merged


def enrich_geovelo_with_length(geovelo_both_years_gdf: gpd.GeoDataFrame) -> pd.DataFrame:
    """
    Add length info to a geovelo gpd. Remove geometry info to turn GeoDataFrame into DataFrame.

    Input columns: 'insee', 'only_2026_geometry', 'only_2021_geometry', 'common_geometry'
    Output columns: 'insee', 'longueur_piste_2026', 'longueur_piste_2021'
    """
    only_2026_length = geovelo_both_years_gdf["only_2026_geometry"].length / 1000
    only_2021_length = geovelo_both_years_gdf["only_2021_geometry"].length / 1000
    common_length = geovelo_both_years_gdf["common_geometry"].length / 1000
    return pd.DataFrame({"longueur_piste_2026": only_2026_length + common_length,
                         "longueur_piste_2021": only_2021_length + common_length,
                         "insee": geovelo_both_years_gdf["insee"]})


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
