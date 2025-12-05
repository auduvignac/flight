import os
import json
import argparse
import pandas as pd
from jinja2 import Template

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

            # Load FI
            fi_df = pd.read_csv(fi_path).sort_values("importance", ascending=False)

            # Load curves
            roc_x, roc_y = load_curve_points(roc_path)
            pr_x, pr_y = load_curve_points(pr_path)

            # Load metadata (ds, th, origin, dest)
            meta = experiments_meta.get(tag, {})

            # Construct human-readable title
            title = (
                f"Dataset {meta.get('ds')} — "
                f"th={meta.get('th')} — "
                f"origin={meta.get('origin')}h — "
                f"dest={meta.get('dest')}h"
            )

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
            })

    # Sort cleanly
    experiments.sort(
        key=lambda e: (
            str(e["ds"]),
            e["th"] or 0,
            e["origin"] or 0,
            e["dest"] or 0
        )
    )

    print(f"📦 Loaded {len(experiments)} experiment(s)")
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