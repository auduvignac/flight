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
# Save matplotlib charts to PNG
# -------------------------------------------------------------------
def save_feature_importances_png(fi_df, exp_title, out_path):
    plt.figure(figsize=(10, 5))
    plt.bar(fi_df["feature"], fi_df["importance"])
    plt.xticks(rotation=45, ha="right")
    plt.title(f"Feature Importances — {exp_title}")
    plt.tight_layout()
    plt.savefig(out_path)
    plt.close()


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
def collect_experiments(input_dir, experiments_meta):
    experiments = []

    print(f"🔍 Searching in: {input_dir}")
    for root, dirs, files in os.walk(input_dir):
        for filename in files:
            if not filename.endswith("_metrics.json"):
                continue
            
            tag = filename.replace("_metrics.json", "")
            exp_path = root
            print(f"➡ Metrics found for {tag}")

            metrics_path = os.path.join(root, filename)
            fi_path = os.path.join(root, tag + "_feature_importances.csv")
            roc_path = os.path.join(root, tag + "_roc_points.json")
            pr_path = os.path.join(root, tag + "_pr_points.json")

            if not os.path.exists(fi_path):
                print(f"⚠ Missing FI for {tag}, skipping.")
                continue

            # Load metrics
            with open(metrics_path, "r") as f:
                metrics = json.load(f)

            fi_df = pd.read_csv(fi_path).sort_values("importance", ascending=False)
            roc_x, roc_y = load_curve_points(roc_path)
            pr_x, pr_y = load_curve_points(pr_path)

            meta = experiments_meta.get(tag, {})
            title = (
                f"Dataset {meta.get('ds')} — th={meta.get('th')} — "
                f"origin={meta.get('origin')}h — dest={meta.get('dest')}h"
            )

            # -------------------------------
            # Save PNG charts
            # -------------------------------
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

            print(f"📸 Saved charts for {tag} in {exp_path}")

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
            })

    return experiments


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
    args = parser.parse_args()

    experiments_meta = load_experiments_metadata()
    experiments = collect_experiments(args.input, experiments_meta)

    if not experiments:
        raise RuntimeError("❌ No experiment results found.")

    render_html(experiments, args.template, args.output)


if __name__ == "__main__":
    main()