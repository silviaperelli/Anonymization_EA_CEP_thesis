import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
from pathlib import Path

DURATION_CSV = "duration_summary.csv"
OPERATOR_ROOT = Path("operator_percentages")
OUTPUT_PDF = "evaluation_tables.pdf"


def get_final_percentages(csv_path):
    df = pd.read_csv(csv_path)
    last10 = df.tail(10)

    return (
        df["pct_filter"].mean() * 100,
        df["pct_map"].mean() * 100,
        df["pct_aggregate"].mean() * 100
    )


def build_table(dataset, scenario, duration_df):

    rows = []

    for ops in ["FO", "FM", "FMA"]:

        if ops == "FO":
            pct_fo, pct_fm, pct_fma = 100, 0, 0
        else:
            op_path = OPERATOR_ROOT / dataset / scenario / ops / "operator_percentages.csv"
            pct_filter, pct_map, pct_aggregate = get_final_percentages(op_path)

            pct_fo = pct_filter
            pct_fm = pct_map
            pct_fma = pct_aggregate

        row = duration_df[
            (duration_df["dataset"] == dataset) &
            (duration_df["scenario"] == scenario) &
            (duration_df["operatorset"] == ops)
        ]

        avg = float(row["avg_duration_secs"].iloc[0])
        std = float(row["std_duration_secs"].iloc[0])

        rows.append([
            ops,
            round(pct_fo, 2),
            round(pct_fm, 2),
            round(pct_fma, 2),
            round(avg, 2),
            round(std, 2)
        ])

    columns = ["Operators", "%F", "%M", "%A", "Avg_Duration (s)", "Std_Duration (s)"]
    return pd.DataFrame(rows, columns=columns)


def add_table_to_pdf(pdf, df, title):

    fig, ax = plt.subplots(figsize=(10, 3))
    ax.axis("off")

    table = ax.table(
        cellText=df.values,
        colLabels=df.columns,
        loc="center"
    )

    table.auto_set_font_size(False)
    table.set_fontsize(10)
    table.scale(1, 1.5)

    plt.title(title, fontsize=14, pad=20)
    pdf.savefig(fig, bbox_inches="tight")
    plt.close()


def main():

    duration_df = pd.read_csv(DURATION_CSV)

    with PdfPages(OUTPUT_PDF) as pdf:

        for dataset in ["airQuality", "geoLife"]:
            for scenario in ["MAX", "Q99"]:

                df = build_table(dataset, scenario, duration_df)

                title = f"{dataset} – {scenario} (3 Objectives)"
                add_table_to_pdf(pdf, df, title)

    print("Saved:", OUTPUT_PDF)


if __name__ == "__main__":
    main()
