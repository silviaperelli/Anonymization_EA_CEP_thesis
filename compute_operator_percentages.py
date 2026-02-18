import pandas as pd
import re
from pathlib import Path

INPUT_CSV = "results/airQuality/3Objectives/MAX_FMA/solutions-percentile.csv"
OUTPUT_CSV = "operator_percentages/airQuality/MAX/FMA/operator_percentages.csv"


def count_operators(pipeline_str):
    """
    Count the number of filter, map and aggregate in a pipeline.
    """
    if pd.isna(pipeline_str):
        return 0, 0, 0

    n_filter = len(re.findall(r'filter\(', pipeline_str))
    n_map = len(re.findall(r'map_', pipeline_str))
    n_aggregate = len(re.findall(r'aggregate\(', pipeline_str))

    return n_filter, n_map, n_aggregate


def extract_pipeline_columns(df):
    """
    Extract all the columns that contain '->solution'
    """
    return [c for c in df.columns if "→solution" in c]


def compute_percentages():
    df = pd.read_csv(INPUT_CSV, sep=";", engine="python")
    df.columns = df.columns.str.strip()

    pipeline_cols = extract_pipeline_columns(df)

    results = []

    generations = sorted(df["n.iterations"].unique())

    for gen in generations:
        df_gen = df[df["n.iterations"] == gen]

        seed_means = []

        for seed in df_gen["randomGenerator.seed"].unique():
            df_seed = df_gen[df_gen["randomGenerator.seed"] == seed]

            pct_filters = []
            pct_maps = []
            pct_aggs = []

            for _, row in df_seed.iterrows():
                for col in pipeline_cols:
                    pipeline = row[col]
                    n_f, n_m, n_a = count_operators(pipeline)

                    total = n_f + n_m + n_a
                    if total == 0:
                        continue

                    pct_filters.append(n_f / total)
                    pct_maps.append(n_m / total)
                    pct_aggs.append(n_a / total)

            if len(pct_filters) > 0:
                seed_means.append((
                    sum(pct_filters) / len(pct_filters),
                    sum(pct_maps) / len(pct_maps),
                    sum(pct_aggs) / len(pct_aggs)
                ))

        if len(seed_means) > 0:
            mean_filter = sum(x[0] for x in seed_means) / len(seed_means)
            mean_map = sum(x[1] for x in seed_means) / len(seed_means)
            mean_agg = sum(x[2] for x in seed_means) / len(seed_means)

            results.append({
                "generation": gen,
                "pct_filter": mean_filter,
                "pct_map": mean_map,
                "pct_aggregate": mean_agg
            })

    out_df = pd.DataFrame(results)

    out_df = out_df.sort_values("generation")

    out_path = Path(OUTPUT_CSV)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    out_df.to_csv(out_path, index=False)

    print("Saved:", OUTPUT_CSV)


if __name__ == "__main__":
    compute_percentages()
