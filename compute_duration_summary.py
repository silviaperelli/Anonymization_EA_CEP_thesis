import pandas as pd
from pathlib import Path

RESULTS_ROOT = Path("results")
OUTPUT_CSV = "duration_summary.csv"


def compute_avg_std_duration(csv_path):
    df = pd.read_csv(
        csv_path,
        sep=";",
        engine="python",
        on_bad_lines="skip",
    )

    df.columns = df.columns.str.strip()

    df["randomGenerator.seed"] = pd.to_numeric(df["randomGenerator.seed"], errors="coerce")
    df["n.iterations"] = pd.to_numeric(df["n.iterations"], errors="coerce")
    df["elapsed.secs"] = pd.to_numeric(df["elapsed.secs"], errors="coerce")

    df = df.dropna(subset=["randomGenerator.seed", "n.iterations", "elapsed.secs"])

    last_rows = (
        df.sort_values(["randomGenerator.seed", "n.iterations"])
          .groupby("randomGenerator.seed")
          .tail(1)
    )

    durations = last_rows["elapsed.secs"]

    avg = durations.mean()
    std = durations.std(ddof=1)

    return avg, std


def main():

    records = []

    for dataset in ["airQuality", "geoLife"]:

        base_path = RESULTS_ROOT / dataset / "3Objectives"

        for folder in base_path.iterdir():

            if not folder.is_dir():
                continue

            name = folder.name

            scenario, operatorset = name.split("_")

            csv_path = folder / "solutions-percentile.csv"

            if not csv_path.exists():
                continue

            avg, std = compute_avg_std_duration(csv_path)

            records.append({
                "dataset": dataset,
                "scenario": scenario,
                "operatorset": operatorset,
                "avg_duration_secs": avg,
                "std_duration_secs": std
            })

    out_df = pd.DataFrame(records)
    out_df = out_df.sort_values(["dataset", "scenario", "operatorset"])

    out_df.to_csv(OUTPUT_CSV, index=False)

    print("Saved:", OUTPUT_CSV)


if __name__ == "__main__":
    main()
