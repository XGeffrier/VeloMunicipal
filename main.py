from data_enricher import merge_top_n_towns, enrich_towns_with_area, enrich_geovelo_with_length, \
    group_geovelo_by_insee_code, enrich_roads_with_total_length
from data_loader import load_geovelo_gpd, load_towns_gpd, load_population_df, load_politics_df, load_roads_df


def main():
    geovelo_2021_gpd, geovelo_2026_gpd = load_geovelo_gpd()
    geovelo_2021_df = group_geovelo_by_insee_code(enrich_geovelo_with_length(geovelo_2021_gpd))
    geovelo_2026_df = group_geovelo_by_insee_code(enrich_geovelo_with_length(geovelo_2026_gpd))
    towns_df = enrich_towns_with_area(load_towns_gpd())
    population_df = load_population_df()
    politics_df = load_politics_df()
    roads_df = enrich_roads_with_total_length(load_roads_df())
    merge_top_n_towns(towns_df, population_df, politics_df, roads_df, geovelo_2021_df, geovelo_2026_df)


if __name__ == '__main__':
    main()
