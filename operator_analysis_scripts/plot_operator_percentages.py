import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

INPUT_CSV = "operator_percentages/geoLife/MAX/FMA/operator_percentages.csv"
TITLE = "Structural Operator Composition – GeoLife (MAX, FMA)"

def plot_percentages(input_csv):
    input_path = Path(input_csv)
    df = pd.read_csv(input_path)

    generations = df["generation"]
    pct_filter = df["pct_filter"]
    pct_map = df["pct_map"]
    pct_aggregate = df["pct_aggregate"]

    plt.figure(figsize=(8, 5))

    plt.plot(generations, pct_filter, label="Filter", linewidth=2)
    plt.plot(generations, pct_map, label="Map", linewidth=2)
    plt.plot(generations, pct_aggregate, label="Aggregate", linewidth=2)

    plt.xlabel("Generation", fontsize=12)
    plt.ylabel("Operator Percentage", fontsize=12)
    plt.title(TITLE, fontsize=14)

    plt.ylim(0, 1)
    plt.legend(fontsize=11)
    plt.grid(True, alpha=0.3)

    output_path = input_path.parent / "operator_percentages_plot.png"
    plt.tight_layout()
    plt.savefig(output_path, dpi=300)
    plt.close()

    print("Saved:", output_path)


if __name__ == "__main__":
    plot_percentages(INPUT_CSV)
