import logging
import os

import pandas as pd

from data_loader import load_postal_df


class Data:
    main_df_path = "data/main_df.parquet"
    _main_df = None
    _postal_df = None

    @classmethod
    def get_main_df(cls):
        if cls._main_df is None:
            if os.path.exists(Data.main_df_path):
                cls._main_df = pd.read_parquet(Data.main_df_path)
            else:
                cls._main_df = cls.build_main_df()
                cls._main_df.to_parquet(Data.main_df_path)
        return cls._main_df

    @classmethod
    def get_postal_df(cls):
        if cls._postal_df is None:
            cls._postal_df = load_postal_df()
        return cls._postal_df

    @staticmethod
    def build_main_df() -> pd.DataFrame:
        """
        Output columns:
        'insee', 'area', 'nom', 'population', 'nuance_politique', 'famille_nuance', 'road_length',
        'bike_lane_length_2021', 'bike_lane_length_2026'
        """
        logging.info("Building main dataframe...")
        from data_enricher import merge_top_n_towns, enrich_towns_with_area, enrich_geovelo_with_length, \
            group_geovelo_by_insee_code, enrich_roads_with_total_length
        from data_loader import load_geovelo_gpd, load_towns_gpd, load_population_df, load_politics_df, load_roads_df
        logging.info("Loading geovelo data...")
        geovelo_2021_gpd, geovelo_2026_gpd = load_geovelo_gpd()
        logging.info("Converting geovelo data...")
        geovelo_2021_df = group_geovelo_by_insee_code(enrich_geovelo_with_length(geovelo_2021_gpd))
        geovelo_2026_df = group_geovelo_by_insee_code(enrich_geovelo_with_length(geovelo_2026_gpd))
        logging.info("Loading other data...")
        towns_df = enrich_towns_with_area(load_towns_gpd())
        population_df = load_population_df()
        politics_df = load_politics_df()
        roads_df = enrich_roads_with_total_length(load_roads_df())
        logging.info("Merging dataframes...")
        return merge_top_n_towns(towns_df, population_df, politics_df, roads_df, geovelo_2021_df, geovelo_2026_df)


def get_all_communes() -> list[dict]:
    """
    Return a list of communes, each commune being a dict with the following keys:
    'zipCode', 'name', 'inseeCode'
    """
    return [{"zipCode": row["code_postal"], "name": row["nom"], "inseeCode": row["insee"]}
            for _, row in Data.get_postal_df().iterrows()]


def get_data_of(insee_code: str):
    main_df = Data.get_main_df()
    row = main_df[main_df['insee'] == insee_code].iloc[0]
    return {'name': row['name'],
            'area': row['area'],
            'population': row['population'],
            'politics_family': row['famille_nuance'],
            'politics_nuance': row['nuance_politique'],
            'road_length': row['road_length'],
            'bike_lane_length_2021': row['bike_lane_length_2021'],
            'bike_lane_length_2026': row['bike_lane_length_2026'],
            'bg_color': '#ffffff',
            'font_color': '#000000'}
