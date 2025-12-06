import os
import json
import argparse
import pandas as pd
from jinja2 import Template

import matplotlib.pyplot as plt


# -------------------------------------------------------------------
# Load experiment metadata
# -------------------------------------------------------------------
def load_experiments_metadata(path=".github/scripts/experiments.json"):
    with open(path, "r") as f:
        data = json.load(f)
    return {exp["tag"]: exp for exp in data}


# -------------------------------------------------------------------
# Load ROC/PR curve points
# -------------------------------------------------------------------
def load_curve_points(path):
    if not path or not os.path.exists(path):
        return [], []
    with open(path, "r") as f:
        pts = json.load(f)
    xs = [p["x"] for p in pts]
    ys = [p["y"] for p in pts]
    return xs, ys


# -------------------------------------------------------------------
# Save full PNG feature importances
# -------------------------------------------------------------------
def save_feature_importances_png(fi_df, exp_title, out_path):
    plt.figure(figsize=(10, 5))
    plt.bar(fi_df["feature"], fi_df["importance"])
    plt.xticks(rotation=45, ha="right")
    plt.title(f"Feature Importances — {exp_title}")
    plt.tight_layout()
    plt.savefig(out_path)
    plt.close()


# -------------------------------------------------------------------
# Save Top-N feature importances (PNG only)
# -------------------------------------------------------------------
def save_top_features_png(fi_df, n, exp_title, out_png):
    """
    Save top-N feature importances as PNG only.
    """
    df_top = fi_df.head(n)

    plt.figure(figsize=(8, 4))
    plt.barh(df_top["feature"], df_top["importance"], color="steelblue")
    plt.gca().invert_yaxis()
    plt.title(f"Top {n} Features — {exp_title}")
    plt.tight_layout()
    plt.savefig(out_png)
    plt.close()


# -------------------------------------------------------------------
# Save ROC/PR curves as PNG
# -------------------------------------------------------------------
def save_curve_png(x, y, xlabel, ylabel, title, out_path):
    if not x or not y:
        return
    plt.figure(figsize=(6, 5))
    plt.plot(x, y)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.title(title)
    plt.tight_layout()
    plt.savefig(out_path)
    plt.close()


# -------------------------------------------------------------------
# Scan ML artifacts and build experiment list
# -------------------------------------------------------------------
def collect_experiments(input_dir, experiments_meta, top_k=10):
    experiments = []

    print(f"🔍 Searching in: {input_dir}")

    # --- ORDERED LOOP ===> FOLLOW experiments.json EXACTLY ---
    for tag, meta in experiments_meta.items():

        exp_path = os.path.join(input_dir, f"ml-results-{tag}")
        print(f"➡ Processing experiment: {tag}")

        metrics_path = os.path.join(exp_path, f"{tag}_metrics.json")
        fi_path      = os.path.join(exp_path, f"{tag}_feature_importances.csv")
        roc_path     = os.path.join(exp_path, f"{tag}_roc_points.json")
        pr_path      = os.path.join(exp_path, f"{tag}_pr_points.json")

        # -----------------------------------------
        # CHECK FILES EXIST (but keep deterministic ordering)
        # -----------------------------------------
        if not os.path.exists(metrics_path):
            print(f"❌ Missing metrics for {tag}, skipping.")
            continue

        if not os.path.exists(fi_path):
            print(f"❌ Missing FI for {tag}, skipping.")
            continue

        # -----------------------------------------
        # LOAD DATA
        # -----------------------------------------
        with open(metrics_path, "r") as f:
            metrics = json.load(f)

        fi_df = pd.read_csv(fi_path).sort_values("importance", ascending=False)

        roc_x, roc_y = load_curve_points(roc_path)
        pr_x, pr_y = load_curve_points(pr_path)

        title = (
            f"Dataset {meta.get('ds')} — th={meta.get('th')} — "
            f"origin={meta.get('origin')}h — dest={meta.get('dest')}h"
        )

        # -----------------------------------------
        # SAVE FIGURES
        # -----------------------------------------
        fi_png = os.path.join(exp_path, f"{tag}_fi.png")
        save_feature_importances_png(fi_df, title, fi_png)

        roc_png = os.path.join(exp_path, f"{tag}_roc.png")
        save_curve_png(
            roc_x, roc_y,
            xlabel="False Positive Rate",
            ylabel="True Positive Rate",
            title=f"ROC Curve — {title}",
            out_path=roc_png
        )

        pr_png = os.path.join(exp_path, f"{tag}_pr.png")
        save_curve_png(
            pr_x, pr_y,
            xlabel="Recall",
            ylabel="Precision",
            title=f"PR Curve — {title}",
            out_path=pr_png
        )

        # Top-K features
        top_png = os.path.join(exp_path, f"{tag}_fi_top{top_k}.png")
        save_top_features_png(fi_df, top_k, title, top_png)

        # -----------------------------------------
        # STORE RESULTS (IN THE EXACT ORDER OF experiments.json)
        # -----------------------------------------
        experiments.append({
            "tag": tag,
            "ds": meta.get("ds"),
            "th": meta.get("th"),
            "origin": meta.get("origin"),
            "dest": meta.get("dest"),
            "title": title,
            "metrics": metrics,
            "fi": fi_df.to_dict("records"),
            "fi_names": fi_df["feature"].tolist(),
            "fi_values": fi_df["importance"].tolist(),
            "roc_x": roc_x,
            "roc_y": roc_y,
            "pr_x": pr_x,
            "pr_y": pr_y,
            "fi_png": fi_png,
            "roc_png": roc_png,
            "pr_png": pr_png,
            "fi_top_png": top_png
        })

    return experiments


# -------------------------------------------------------------------
# Global summary plots
# -------------------------------------------------------------------
def generate_global_plots(experiments, out_dir):
    os.makedirs(out_dir, exist_ok=True)

    rows = []
    for exp in experiments:
        rows.append({
            "ds": exp["ds"],
            "th": exp["th"],
            "origin": exp["origin"],
            "dest": exp["dest"],
            "accuracy": exp["metrics"].get("accuracy")
        })

    df = pd.DataFrame(rows)

    # -------------------- 1. origin plot --------------------
    df1 = df[(df.ds == "D2") & (df.th == 60) & (df.dest == 0)]
    df1 = df1.sort_values("origin")

    plt.figure(figsize=(7,5))
    plt.plot(df1.origin, df1.accuracy, marker="o")
    plt.xlabel("Fenêtre temporelle départ (origin h)")
    plt.ylabel("Accuracy")
    plt.title("Accuracy vs origin — D2, th=60, dest=0")
    plt.grid(True)
    plot1 = os.path.join(out_dir, "accuracy_vs_origin.png")
    plt.savefig(plot1, dpi=300, bbox_inches='tight')
    plt.close()

    # -------------------- 2. dest plot --------------------
    df2 = df[(df.ds == "D2") & (df.th == 60) & (df.origin == 0)]
    df2 = df2.sort_values("dest")

    plt.figure(figsize=(7,5))
    plt.plot(df2.dest, df2.accuracy, marker="o")
    plt.xlabel("Fenêtre temporelle arrivée (dest h)")
    plt.ylabel("Accuracy")
    plt.title("Accuracy vs dest — D2, th=60, origin=0")
    plt.grid(True)
    plot2 = os.path.join(out_dir, "accuracy_vs_dest.png")
    plt.savefig(plot2, dpi=300, bbox_inches='tight')
    plt.close()

    # -------------------- 3. threshold plot --------------------
    df3 = df[(df.ds == "D2") & (df.origin == 7) & (df.dest == 7)]
    df3 = df3.sort_values("th")

    plt.figure(figsize=(7,5))
    plt.plot(df3.th, df3.accuracy, marker="o")
    plt.xlabel("Seuil (minutes)")
    plt.ylabel("Accuracy")
    plt.title("Accuracy vs threshold — D2, origin=7, dest=7")
    plt.grid(True)
    plot3 = os.path.join(out_dir, "accuracy_vs_th.png")
    plt.savefig(plot3, dpi=300, bbox_inches='tight')
    plt.close()

    # -------------------- 4. dataset plot --------------------
    df4 = df[(df.origin == 7) & (df.dest == 7) & (df.th == 60)]
    df4 = df4.sort_values("ds")

    plt.figure(figsize=(7,5))
    plt.plot(df4.ds, df4.accuracy, marker="o")
    plt.xlabel("Dataset")
    plt.ylabel("Accuracy")
    plt.title("Accuracy vs dataset — origin=7, dest=7, th=60")
    plt.grid(True)
    plot4 = os.path.join(out_dir, "accuracy_vs_dataset.png")
    plt.savefig(plot4, dpi=300, bbox_inches='tight')
    plt.close()

    print("📊 Global plots generated:")
    print(" -", plot1)
    print(" -", plot2)
    print(" -", plot3)
    print(" -", plot4)

    return {
        "accuracy_vs_origin": plot1,
        "accuracy_vs_dest": plot2,
        "accuracy_vs_th": plot3,
        "accuracy_vs_ds": plot4
    }


# -------------------------------------------------------------------
# Render HTML using Jinja2
# -------------------------------------------------------------------
def render_html(experiments, template_path, output_path):
    with open(template_path, "r") as f:
        template = Template(f.read())

    html = template.render(experiments=experiments)

    with open(output_path, "w") as f:
        f.write(html)

    print(f"📄 Global report generated: {output_path}")


# -------------------------------------------------------------------
# Main
# -------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="Directory with ML artifacts")
    parser.add_argument("--output", required=True, help="Output HTML report")
    parser.add_argument("--template", required=True, help="HTML Jinja template")
    parser.add_argument("--topk", type=int, default=10, help="Top-K features to export")
    args = parser.parse_args()

    experiments_meta = load_experiments_metadata()
    experiments = collect_experiments(args.input, experiments_meta, top_k=args.topk)

    if not experiments:
        raise RuntimeError("❌ No experiment results found.")

    # NEW: global plots
    generate_global_plots(experiments, out_dir="global-plots")

    render_html(experiments, args.template, args.output)


if __name__ == "__main__":
    main()