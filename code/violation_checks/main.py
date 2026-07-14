# compare_baseline.py
import json
import boto3
import math
from urllib.parse import urlparse

s3 = boto3.client("s3")

_PROBLEM_TYPE_CONSTRAINTS_KEY = {
    "BinaryClassification": "binary_classification_constraints",
    "MulticlassClassification": "multiclass_classification_constraints",
    "Regression": "regression_constraints",
}

_PROBLEM_TYPE_METRICS_KEY = {
    "BinaryClassification": "binary_classification_metrics",
    "MulticlassClassification": "multiclass_classification_metrics",
    "Regression": "regression_metrics",
}

def _s3_uri_to_bucket_key(s3_uri):
    parsed = urlparse(s3_uri)
    return parsed.netloc, parsed.path.lstrip("/")


def _load_json_from_s3(s3_uri):
    bucket, key = _s3_uri_to_bucket_key(s3_uri)
    obj = s3.get_object(Bucket=bucket, Key=key)
    return json.loads(obj["Body"].read())


# ---------------------------------------------------------------------------
# Data Quality
#
# Two files now feed this comparator:
#   constraints.json — schema-level checks (features[].inferred_type,
#                       completeness, num_constraints) + monitoring_config,
#                       which carries the distribution comparison_threshold.
#   statistics.json   — per-feature numerical_statistics (mean, std_dev, min,
#                       max), needed to actually detect distribution drift.
# ---------------------------------------------------------------------------

def _get_distribution_comparison_threshold(baseline_constraints, comparison_threshold):
    return (
        baseline_constraints
        .get("monitoring_config", {})
        .get("distribution_constraints", {})
        .get("comparison_threshold", comparison_threshold)
    )


def _compare_data_quality_schema(current_constraints, baseline_constraints):
    """features[] schema — column-level checks (type, completeness, boolean constraints)."""
    violations = []
    current_features = {f["name"]: f for f in current_constraints.get("features", [])}
    baseline_features = {f["name"]: f for f in baseline_constraints.get("features", [])}

    for name, baseline_feature in baseline_features.items():
        current_feature = current_features.get(name)
        if current_feature is None:
            violations.append({
                "feature": name,
                "constraint_check_type": "missing_column",
                "description": f"Column '{name}' present in baseline but missing from current dataset.",
            })
            continue

        if current_feature.get("inferred_type") != baseline_feature.get("inferred_type"):
            violations.append({
                "feature": name,
                "constraint_check_type": "data_type_check",
                "description": (
                    f"Type changed from '{baseline_feature.get('inferred_type')}' "
                    f"to '{current_feature.get('inferred_type')}'."
                ),
            })

        baseline_completeness = baseline_feature.get("completeness", 1.0)
        current_completeness = current_feature.get("completeness", 1.0)
        if current_completeness < baseline_completeness:
            violations.append({
                "feature": name,
                "constraint_check_type": "completeness_check",
                "description": f"Completeness dropped from {baseline_completeness} to {current_completeness}.",
            })

        for constraint_name, baseline_value in baseline_feature.get("num_constraints", {}).items():
            current_value = current_feature.get("num_constraints", {}).get(constraint_name)
            if baseline_value is True and current_value is False:
                violations.append({
                    "feature": name,
                    "constraint_check_type": constraint_name,
                    "description": f"Constraint '{constraint_name}' held in baseline but violated in current data.",
                })

    return violations


def _compare_data_quality_distribution(current_statistics, baseline_statistics, comparison_threshold):
    """
    Numeric distribution drift, using numerical_statistics.mean from statistics.json.
    NOTE: this is our own relative mean-shift approximation, not AWS's internal
    "Simple"/"Robust" comparison_method — that algorithm isn't publicly documented.
    """
    violations = []
    current_features = {f["name"]: f for f in current_statistics.get("features", [])}
    baseline_features = {f["name"]: f for f in baseline_statistics.get("features", [])}

    for name, baseline_feature in baseline_features.items():
        baseline_num_stats = baseline_feature.get("numerical_statistics")
        if baseline_num_stats is None:
            continue  # not a numeric feature (e.g. string) — nothing to compare here

        current_feature = current_features.get(name)
        if current_feature is None:
            continue  # already flagged as missing_column by the schema check

        current_num_stats = current_feature.get("numerical_statistics")
        if current_num_stats is None:
            continue

        baseline_mean = baseline_num_stats.get("mean")
        current_mean = current_num_stats.get("mean")
        if baseline_mean is None or current_mean is None:
            continue

        denominator = abs(baseline_mean) if baseline_mean != 0 else (baseline_num_stats.get("std_dev") or 1.0)
        relative_shift = abs(current_mean - baseline_mean) / denominator

        if relative_shift > comparison_threshold:
            violations.append({
                "feature": name,
                "constraint_check_type": "baseline_drift_check",
                "description": (
                    f"Mean shifted from {baseline_mean} to {current_mean} "
                    f"(relative shift {relative_shift:.4f} exceeds threshold {comparison_threshold})."
                ),
            })

    return violations


def dq_handler(event, context):
    """
    Expected event shape:
    {
        "current_constraints_uri": "s3://.../constraints.json",
        "baseline_constraints_uri": "s3://.../constraints.json",
        "current_statistics_uri": "s3://.../statistics.json",
        "baseline_statistics_uri": "s3://.../statistics.json",
        "fail_on_violation": true
        "comparison_threshold": 0.1
    }
    """
    current_constraints = _load_json_from_s3(event["current_constraints_uri"])
    baseline_constraints = _load_json_from_s3(event["baseline_constraints_uri"])
    current_statistics = _load_json_from_s3(event["current_statistics_uri"])
    baseline_statistics = _load_json_from_s3(event["baseline_statistics_uri"])
    fail_on_violation = event.get("fail_on_violation", True)
    comparison_threshold = event.get("comparison_threshold", 0.1)

    comparison_threshold = _get_distribution_comparison_threshold(baseline_constraints, comparison_threshold)

    violations = _compare_data_quality_schema(current_constraints, baseline_constraints)
    violations += _compare_data_quality_distribution(current_statistics, baseline_statistics, comparison_threshold)

    passed = len(violations) == 0
    return {
        "passed": passed,
        "violation_count": len(violations),
        "violations": violations,
        "fail_on_violation": fail_on_violation,
        "should_fail_pipeline": fail_on_violation and not passed,
    }



# ---------------------------------------------------------------------------
# Model Quality
#
# The rule (threshold + comparison_operator) comes from the BASELINE's
# constraints.json. The value being checked comes from the CURRENT run's
# statistics.json — not its own constraints.json, since constraints.json's
# "threshold" field is a suggested rule, not guaranteed to equal the raw
# observed metric value for that run.
# ---------------------------------------------------------------------------

def _compare_model_quality(current_statistics, baseline_constraints, problem_type):
    constraints_key = _PROBLEM_TYPE_CONSTRAINTS_KEY.get(problem_type)
    metrics_key = _PROBLEM_TYPE_METRICS_KEY.get(problem_type)
    if constraints_key is None or metrics_key is None:
        raise ValueError(f"Unknown problem_type: {problem_type}")

    violations = []
    baseline_metrics = baseline_constraints.get(constraints_key, {})
    current_metrics = current_statistics.get(metrics_key, {})

    for metric_name, baseline_metric in baseline_metrics.items():
        current_metric = current_metrics.get(metric_name)
        if current_metric is None:
            violations.append({
                "metric_name": metric_name,
                "constraint_check_type": "missing_metric",
                "description": f"Metric '{metric_name}' present in baseline but missing from current run.",
            })
            continue

        baseline_threshold = baseline_metric.get("threshold")
        current_value = current_metric.get("value")
        operator = baseline_metric.get("comparison_operator")

        is_violation = (
            (operator == "LessThanThreshold" and current_value < baseline_threshold) or
            (operator == "GreaterThanThreshold" and current_value > baseline_threshold)
        )
        if is_violation:
            violations.append({
                "metric_name": metric_name,
                "constraint_check_type": "model_quality_drift_check",
                "description": (
                    f"Value {current_value} violates baseline threshold {baseline_threshold} "
                    f"({operator})."
                ),
            })

    return violations


def mq_handler(event, context):
    """
    Expected event shape:
    {
        "problem_type": "BinaryClassification",
        "current_statistics_uri": "s3://.../statistics.json", # current stats compared to baseline constraints
        "baseline_constraints_uri": "s3://.../constraints.json",
        "fail_on_violation": true
    }
    """
    current_statistics = _load_json_from_s3(event["current_statistics_uri"])
    baseline_constraints = _load_json_from_s3(event["baseline_constraints_uri"])
    fail_on_violation = event.get("fail_on_violation", True)

    violations = _compare_model_quality(current_statistics, baseline_constraints, event["problem_type"])

    passed = len(violations) == 0
    return {
        "passed": passed,
        "violation_count": len(violations),
        "violations": violations,
        "fail_on_violation": fail_on_violation,
        "should_fail_pipeline": fail_on_violation and not passed,
    }

# ---------------------------------------------------------------------------
# Data Bias / Model Bias
#
# Both share the same analysis.json shape:
#   { "pre_training_bias_metrics": {"facets": {...}}, "post_training_bias_metrics": {"facets": {...}} }
# A data-bias-only job will typically only populate pre_training_bias_metrics;
# a model-bias job typically populates post_training_bias_metrics (sometimes both).
# This comparator handles whichever sections are present in the baseline file.
#
# Rule (per AWS docs, clarify-model-monitor-bias-drift-violations):
#   violation if abs(current_value) > abs(baseline_value)
#   i.e. "farther from zero than the baseline constraint" = worse.
# ---------------------------------------------------------------------------

_BIAS_SECTIONS = ["pre_training_bias_metrics", "post_training_bias_metrics"]


def _index_bias_metrics(analysis_json):
    """
    Flattens analysis.json's bias sections into:
      { (section, facet_name, value_or_threshold, metric_name): value_or_None }
    """
    index = {}
    for section in _BIAS_SECTIONS:
        facets = analysis_json.get(section, {}).get("facets", {})
        for facet_name, facet_entries in facets.items():
            for entry in facet_entries:
                value_or_threshold = str(entry.get("value_or_threshold"))
                for metric in entry.get("metrics", []):
                    key = (section, facet_name, value_or_threshold, metric.get("name"))
                    value = metric.get("value")
                    index[key] = value if isinstance(value, (int, float)) else None
    return index


def _compare_bias(current, baseline):
    violations = []
    baseline_index = _index_bias_metrics(baseline)
    current_index = _index_bias_metrics(current)

    for key, baseline_value in baseline_index.items():
        section, facet_name, value_or_threshold, metric_name = key

        if baseline_value is None:
            continue  # baseline metric wasn't computed (error/null) — nothing to compare against

        current_value = current_index.get(key)
        if current_value is None:
            violations.append({
                "facet": facet_name,
                "facet_value": value_or_threshold,
                "metric_name": metric_name,
                "constraint_check_type": "missing_metric",
                "description": (
                    f"Metric '{metric_name}' for facet '{facet_name}'={value_or_threshold} "
                    f"present in baseline ({section}) but missing or not computed in current run."
                ),
            })
            continue

        if abs(current_value) > abs(baseline_value):
            violations.append({
                "facet": facet_name,
                "facet_value": value_or_threshold,
                "metric_name": metric_name,
                "constraint_check_type": "bias_drift_check",
                "description": (
                    f"Value {current_value} does not meet the constraint requirement "
                    f"of baseline value {baseline_value} for metric '{metric_name}' ({section})."
                ),
            })

    return violations


def mb_handler(event, context):
    """
    Expected event shape:
    {
        "current_analysis_uri": "s3://.../analysis.json",
        "baseline_analysis_uri": "s3://.../analysis.json",
        "fail_on_violation": true
    }
    """
    current = _load_json_from_s3(event["current_analysis_uri"])
    baseline = _load_json_from_s3(event["baseline_analysis_uri"])
    fail_on_violation = event.get("fail_on_violation", True)

    violations = _compare_bias(current, baseline)

    passed = len(violations) == 0
    return {
        "PASSED": passed,
        "VIOLATION_COUNT": len(violations),
        "VIOLATIONS": violations,
        "FAIL_ON_VIOLATION": fail_on_violation,
        "SHOULD_FAIL_PIPELINE": fail_on_violation and not passed,
    }



# ---------------------------------------------------------------------------
# Model Explainability (SHAP feature attribution drift)
#
# analysis.json shape:
#   { "explanations": { "kernel_shap": { "<label>": { "global_shap_values": {feature: value, ...} } } } }
#
# Rule (per AWS docs, clarify-model-monitor-feature-attribution-drift):
#   For each label, compute NDCG comparing the current feature-attribution ranking
#   against the baseline ranking. Violation if NDCG < 0.90.
#
#   F  = baseline features sorted descending by baseline attribution score
#   F' = current features sorted descending by current attribution score
#   a(f) = feature f's BASELINE attribution score (always looked up from baseline)
#   DCG  = sum_i a(f'_i) / log2(i+1)   (current ranking, baseline scores)
#   iDCG = sum_i a(f_i)  / log2(i+1)   (baseline ranking, baseline scores — the ideal/max case)
#   NDCG = DCG / iDCG
# ---------------------------------------------------------------------------

def _ndcg(baseline_shap, current_shap):
    features_by_baseline_rank = sorted(baseline_shap.keys(), key=lambda f: baseline_shap[f], reverse=True)
    features_by_current_rank = sorted(current_shap.keys(), key=lambda f: current_shap.get(f, 0.0), reverse=True)

    dcg = sum(
        baseline_shap.get(feature, 0.0) / math.log2(i + 1)
        for i, feature in enumerate(features_by_current_rank, start=1)
    )
    idcg = sum(
        baseline_shap[feature] / math.log2(i + 1)
        for i, feature in enumerate(features_by_baseline_rank, start=1)
    )

    if idcg == 0:
        return 1.0  # degenerate case (all-zero baseline attributions) — nothing to normalize against

    return dcg / idcg


def _compare_explainability(current, baseline, ndcg_violation_threshold):
    violations = []
    baseline_labels = baseline.get("explanations", {}).get("kernel_shap", {})
    current_labels = current.get("explanations", {}).get("kernel_shap", {})

    for label, baseline_label_data in baseline_labels.items():
        baseline_shap = baseline_label_data.get("global_shap_values", {})
        current_label_data = current_labels.get(label)

        if current_label_data is None:
            violations.append({
                "label": label,
                "metric_name": "shap",
                "constraint_check_type": "missing_label",
                "description": f"Label '{label}' present in baseline but missing from current run.",
            })
            continue

        current_shap = current_label_data.get("global_shap_values", {})
        ndcg_score = _ndcg(baseline_shap, current_shap)

        if ndcg_score < ndcg_violation_threshold:
            violations.append({
                "label": label,
                "metric_name": "shap",
                "constraint_check_type": "feature_attribution_drift_check",
                "description": (
                    f"Feature attribution drift {ndcg_score} exceeds threshold "
                    f"{ndcg_violation_threshold}"
                ),
            })

    return violations


def me_handler(event, context):
    """
    Expected event shape:
    {
        "current_analysis_uri": "s3://.../analysis.json",
        "baseline_analysis_uri": "s3://.../analysis.json",
        "fail_on_violation": true
    }
    """
    current = _load_json_from_s3(event["current_analysis_uri"])
    baseline = _load_json_from_s3(event["baseline_analysis_uri"])
    fail_on_violation = event.get("fail_on_violation", True)
    ndcg_violation_threshold = event.get("ndcg_violation_threshold", 0.90)

    violations = _compare_explainability(current, baseline, ndcg_violation_threshold)
    passed = len(violations) == 0
    return {
        "PASSED": passed,
        "VIOLATION_COUNT": len(violations),
        "VIOLATIONS": violations,
        "FAIL_ON_VIOLATION": fail_on_violation,
        "SHOULD_FAIL_PIPELINE": fail_on_violation and not passed,
    }