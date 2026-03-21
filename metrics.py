"""
This file is not used in prod, only offline to produce data metrics.
"""
import pandas as pd

from data_loader import DataLoader


def ratio_improvement_by_party(df: pd.DataFrame) -> pd.DataFrame:
    df["evolution"] = df["longueur_piste_2026"] - df["longueur_piste_2021"]
    df = df[["nuance_politique_complete", "evolution", "longueur_route"]]
    df = df.groupby("nuance_politique_complete").sum().reset_index()
    df["improvement"] = df["evolution"] / df["longueur_route"]
    df = df[["nuance_politique_complete", "improvement"]]
    df = df.sort_values(by="improvement", ascending=False)
    return df


def bike_ratio_2026_by_party(df: pd.DataFrame) -> pd.DataFrame:
    """
    Two ways: either compute ratio by town then average, or total ratio accros town.
    Second one more interesting I think, but first one easier to explain to large audience.
    """
    df["ratio"] = df["longueur_piste_2026"] / df["longueur_route"]
    df = df[["nuance_politique_complete", "ratio"]]
    df = df.groupby("nuance_politique_complete").mean().reset_index()
    df = df.sort_values(by="ratio", ascending=False)
    return df


def bike_ratio_2026_by_town(df: pd.DataFrame) -> pd.DataFrame:
    df["ratio"] = df["longueur_piste_2026"] / df["longueur_route"]
    df = df[["insee", "nom", "ratio"]].groupby(["insee", "nom"]).mean().reset_index()
    return df.sort_values(by="ratio", ascending=False)


complete_df = DataLoader.get_merged_df()
complete_df = complete_df.drop_duplicates(subset='insee', keep="last")
cities_100k = complete_df[complete_df["population"] >= 100_000]
towns_10k = complete_df[complete_df["population"] >= 10_000]

avg_ratio_2026 = complete_df["longueur_piste_2026"].sum() / complete_df["longueur_route"].sum()
avg_cities_ratio_2026 = cities_100k["longueur_piste_2026"].sum() / cities_100k["longueur_route"].sum()
avg_towns_ratio_2026 = towns_10k["longueur_piste_2026"].sum() / towns_10k["longueur_route"].sum()
bike_lane_created_length = complete_df["longueur_piste_2026"].sum() - complete_df["longueur_piste_2021"].sum()
total_road_length = complete_df["longueur_route"].sum()
ratio_by_party_cities = bike_ratio_2026_by_party(cities_100k)
cities_top = bike_ratio_2026_by_town(cities_100k)
