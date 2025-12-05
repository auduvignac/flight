import json
import os
import argparse
import pandas as pd
from jinja2 import Template


def load_json(path):
    """Load a JSON file."""
    with open(path) as f:
        return json.load(f)


def load_feature_importances(path):
    """Load feature importances CSV as sorted dict list."""
    df = pd.read_csv(path)
    df = df.sort_values("importance", ascending=False)
    return df


def load_curve_points(path):
    """Load ROC/PR JSON points if file exists."""
    if not os.path.exists(path):
        return [], []
    with open(path) as f:
        pts = json.load(f)
    xs = [p["x"] for p in pts]
    ys = [p["y"] for p in pts]
    return xs, ys


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="Directory containing artifacts grouped by experiment")
    parser.add_argument("--output", required=True, help="HTML file output")
    parser.add_argument("--template", required=True, help="HTML Jinja template")
    args = parser.parse_args()

    exp_root = args.input
    experiments = []

    print(f"🔍 Scanning experiments in: {exp_root}")

    for exp_name in sorted(os.listdir(exp_root)):
        exp_path = os.path.join(exp_root, exp_name)
        if not os.path.isdir(exp_path):
            continue

        print(f"➡ Found experiment folder: {exp_name}")

        # Identify file paths
        metrics_path = None
        fi_path = None
        roc_path = None
        pr_path = None

        for f in os.listdir(exp_path):
            if f.endswith("_metrics.json"):
                metrics_path = os.path.join(exp_path, f)
            elif f.endswith("_feature_importances.csv"):
                fi_path = os.path.join(exp_path, f)
            elif f.endswith("_roc_points.json"):
                roc_path = os.path.join(exp_path, f)
            elif f.endswith("_pr_points.json"):
                pr_path = os.path.join(exp_path, f)

        if metrics_path is None or fi_path is None:
            print(f"⚠ Missing metrics or feature importances for {exp_name}, skipping.")
            continue

        metrics = load_json(metrics_path)
        fi = load_feature_importances(fi_path)

        roc_x, roc_y = load_curve_points(roc_path) if roc_path else ([], [])
        pr_x, pr_y = load_curve_points(pr_path) if pr_path else ([], [])

        experiments.append({
            "name": exp_name,
            "metrics": metrics,
            "fi": fi.to_dict("records"),
            "fi_names": list(fi["feature"]),
            "fi_values": list(fi["importance"]),
            "roc_x": roc_x,
            "roc_y": roc_y,
            "pr_x": pr_x,
            "pr_y": pr_y
        })

    print(f"📦 Loaded {len(experiments)} experiment results.")

    # Load template
    with open(args.template) as f:
        template = Template(f.read())

    html = template.render(experiments=experiments)

    with open(args.output, "w") as f:
        f.write(html)

    print(f"📄 Global report generated: {args.output}")


if __name__ == "__main__":
    main()