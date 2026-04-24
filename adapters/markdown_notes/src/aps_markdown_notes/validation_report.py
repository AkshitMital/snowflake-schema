"""Pilot QA harness and markdown report rendering for APS content validation."""

from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from .normalize import normalize_entity_name
from .staging_writer import MergeState

STRUCTURAL_SCHEMA_CONTRACT_CODES = {
    "DUPLICATE_CONTENT_ITEM_KEY",
    "DUPLICATE_CONTENT_VERSION_KEY",
    "MULTIPLE_CURRENT_VERSION_FLAGS",
    "MISSING_CURRENT_VERSION_POINTER",
    "MULTIPLE_CURRENT_VERSION_POINTERS",
    "DUPLICATE_VERSION_MARKED_CURRENT",
    "CURRENT_POINTER_FLAG_MISMATCH",
}
ADAPTER_BLOCKER_CODES = {
    "MISSING_PRIMARY_BODY",
    "MISSING_INVESTMENT_BODY_ANCHORS",
}
ALLOWED_OPERATIONAL_DATE_TYPES = {
    "MEETING_DATE",
    "REPORT_PERIOD_END",
    "REPORT_PERIOD_START",
    "EFFECTIVE_DATE_CANDIDATE",
    "SOURCE_MODIFIED",
    "SOURCE_CREATED",
    "INGESTED",
}
DATE_PRIORITY = {
    "MEETING_DATE": 10,
    "REPORT_PERIOD_END": 20,
    "REPORT_PERIOD_START": 30,
    "EFFECTIVE_DATE_CANDIDATE": 40,
    "SOURCE_MODIFIED": 50,
    "SOURCE_CREATED": 60,
    "INGESTED": 70,
}


@dataclass(frozen=True)
class ReviewIssue:
    content_id: str
    content_version_id: str | None
    issue_code: str
    severity: str
    issue_detail: str
    issue_context: dict[str, Any] = field(default_factory=dict)
    source_title: str | None = None
    bundle_id: str | None = None


@dataclass(frozen=True)
class ValidationExample:
    source_title: str
    bundle_id: str | None
    entity_type: str | None
    expected_entity: str | None
    actual_entity: str | None
    reason: str


@dataclass(frozen=True)
class BlockedNote:
    content_id: str
    source_title: str
    issue_codes: tuple[str, ...]
    severity: str
    issue_details: tuple[str, ...]


@dataclass
class ValidationSummary:
    report_title: str
    generated_at: str
    notes_evaluated: int
    labeled_bundle_count: int
    recommendation: str
    recommendation_rationale: list[str]
    issue_counts: dict[str, int]
    missing_body_anchor_counts: dict[str, int]
    derived_note_counts: dict[str, int]
    unresolved_entity_mention_count: int
    missing_operational_date_count: int
    issues: list[ReviewIssue]
    blocked_notes: list[BlockedNote]
    false_positive_examples: list[ValidationExample]
    false_negative_examples: list[ValidationExample]


def build_local_validation_summary(
    state: MergeState,
    *,
    manifest: dict[str, Any] | None = None,
    report_title: str = "APS Markdown Note Pilot Results",
    review_issues: Iterable[ReviewIssue] | None = None,
) -> ValidationSummary:
    current_contexts = _current_contexts(state)
    local_issues = list(review_issues or [])
    local_issues.extend(_derive_local_review_issues(current_contexts))
    issue_counts = dict(Counter(issue.issue_code for issue in local_issues))
    unresolved_count = issue_counts.get("UNRESOLVED_ENTITY_MENTIONS", 0)
    missing_operational_dates = issue_counts.get("MISSING_OPERATIONAL_DATE", 0)
    missing_body_anchor_counts = {
        "investment": issue_counts.get("MISSING_INVESTMENT_BODY_ANCHORS", 0),
        "portfolio": issue_counts.get("MISSING_PORTFOLIO_BODY_ANCHORS", 0),
        "strategy": issue_counts.get("MISSING_STRATEGY_BODY_ANCHORS", 0),
    }
    derived_note_counts = _derive_local_note_counts(current_contexts)
    false_positive_examples, false_negative_examples, labeled_bundle_count = _compare_expected_entities(
        manifest=manifest,
        current_contexts=current_contexts,
    )
    recommendation, rationale = derive_recommendation(
        issues=local_issues,
        false_positive_examples=false_positive_examples,
        false_negative_examples=false_negative_examples,
        labeled_bundle_count=labeled_bundle_count,
    )
    blocked_notes = _build_blocked_notes(local_issues)

    return ValidationSummary(
        report_title=report_title,
        generated_at=_utc_now_isoformat(),
        notes_evaluated=len(current_contexts),
        labeled_bundle_count=labeled_bundle_count,
        recommendation=recommendation,
        recommendation_rationale=rationale,
        issue_counts=issue_counts,
        missing_body_anchor_counts=missing_body_anchor_counts,
        derived_note_counts=derived_note_counts,
        unresolved_entity_mention_count=unresolved_count,
        missing_operational_date_count=missing_operational_dates,
        issues=local_issues,
        blocked_notes=blocked_notes,
        false_positive_examples=false_positive_examples,
        false_negative_examples=false_negative_examples,
    )


def derive_recommendation(
    *,
    issues: list[ReviewIssue],
    false_positive_examples: list[ValidationExample],
    false_negative_examples: list[ValidationExample],
    labeled_bundle_count: int,
) -> tuple[str, list[str]]:
    issue_codes = {issue.issue_code for issue in issues}

    if issue_codes & STRUCTURAL_SCHEMA_CONTRACT_CODES:
        return (
            "needs schema/contract fixes",
            [
                "Structural content-contract or current-version safety issues remain present.",
                "App build should wait until duplicate-key and current-pointer semantics are trustworthy.",
            ],
        )

    if any(issue.severity == "P1" for issue in issues):
        return (
            "needs adapter fixes",
            [
                "Pilot data still produces P1 adapter-quality issues such as missing primary bodies or missing investment anchors.",
                "The review app should not sit on top of data that is still blocked from promotion.",
            ],
        )

    if labeled_bundle_count == 0:
        return (
            "needs adapter fixes",
            [
                "No labeled subset was supplied for false-positive/false-negative measurement.",
                "The QA harness is ready, but the adapter has not yet earned trust on measured precision and recall.",
            ],
        )

    if false_positive_examples or false_negative_examples:
        return (
            "needs adapter fixes",
            [
                "The labeled subset still shows entity-link false positives and/or false negatives.",
                "Resolve entity-link quality issues before using the review app as a primary operational surface.",
            ],
        )

    return (
        "ready for app build",
        [
            "No structural or adapter-blocking issues remain in the evaluated set.",
            "The labeled subset does not show entity-link misses or false positives, so the review app can move forward on the validated pilot scope.",
        ],
    )


def render_validation_report(summary: ValidationSummary) -> str:
    lines: list[str] = [
        f"# {summary.report_title}",
        "",
        "## Recommendation",
        "",
        f"- Status: `{summary.recommendation}`",
        f"- Notes evaluated: `{summary.notes_evaluated}`",
        f"- Labeled bundles evaluated: `{summary.labeled_bundle_count}`",
        f"- Report generated at: `{summary.generated_at}`",
        "",
        "### Rationale",
    ]
    lines.extend(f"- {reason}" for reason in summary.recommendation_rationale)
    lines.extend(
        [
            "",
            "## Issue Summary",
            "",
            f"- Duplicate item key issues: `{summary.issue_counts.get('DUPLICATE_CONTENT_ITEM_KEY', 0)}`",
            f"- Duplicate version key issues: `{summary.issue_counts.get('DUPLICATE_CONTENT_VERSION_KEY', 0)}`",
            f"- Missing primary body issues: `{summary.issue_counts.get('MISSING_PRIMARY_BODY', 0)}`",
            f"- Unresolved entity mention issues: `{summary.issue_counts.get('UNRESOLVED_ENTITY_MENTIONS', 0)}`",
            f"- Missing investment body anchor issues: `{summary.issue_counts.get('MISSING_INVESTMENT_BODY_ANCHORS', 0)}`",
            f"- Missing portfolio body anchor issues: `{summary.issue_counts.get('MISSING_PORTFOLIO_BODY_ANCHORS', 0)}`",
            f"- Missing strategy body anchor issues: `{summary.issue_counts.get('MISSING_STRATEGY_BODY_ANCHORS', 0)}`",
            f"- Missing operational date issues: `{summary.issue_counts.get('MISSING_OPERATIONAL_DATE', 0)}`",
            "",
            "## Derived Note Counts",
            "",
            f"- Investment notes: `{summary.derived_note_counts.get('investment', 0)}`",
            f"- Portfolio notes: `{summary.derived_note_counts.get('portfolio', 0)}`",
            f"- Strategy notes: `{summary.derived_note_counts.get('strategy', 0)}`",
            "",
            "## Duplicate Item/Version Key Issues",
            "",
        ]
    )
    lines.extend(_issue_examples(summary.issues, {"DUPLICATE_CONTENT_ITEM_KEY", "DUPLICATE_CONTENT_VERSION_KEY"}))
    lines.extend(["", "## Missing Primary Body Issues", ""])
    lines.extend(_issue_examples(summary.issues, {"MISSING_PRIMARY_BODY"}))
    lines.extend(["", "## Unresolved Entity Mentions", ""])
    lines.extend(_issue_examples(summary.issues, {"UNRESOLVED_ENTITY_MENTIONS"}))
    lines.extend(["", "## Missing Body Anchors", ""])
    lines.extend(
        _issue_examples(
            summary.issues,
            {
                "MISSING_INVESTMENT_BODY_ANCHORS",
                "MISSING_PORTFOLIO_BODY_ANCHORS",
                "MISSING_STRATEGY_BODY_ANCHORS",
            },
        )
    )
    lines.extend(["", "## Missing Operational Dates", ""])
    lines.extend(_issue_examples(summary.issues, {"MISSING_OPERATIONAL_DATE"}))
    lines.extend(["", "## False Positive Entity-Link Examples", ""])
    lines.extend(_example_lines(summary.false_positive_examples))
    lines.extend(["", "## False Negative Entity-Link Examples", ""])
    lines.extend(_example_lines(summary.false_negative_examples))
    lines.extend(["", "## Notes Blocked From App Promotion By P1 Issues", ""])
    lines.extend(_blocked_note_lines(summary.blocked_notes))
    return "\n".join(lines).rstrip() + "\n"


def write_validation_report(summary: ValidationSummary, output_path: str | Path) -> Path:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(render_validation_report(summary), encoding="utf-8")
    return path


def _current_contexts(state: MergeState) -> list[dict[str, Any]]:
    contexts: list[dict[str, Any]] = []
    for content_item_key, pointer in state.content_item_current_version.items():
        item = state.content_items[content_item_key]
        version = state.content_versions[pointer["CURRENT_CONTENT_VERSION_KEY"]]
        content_id = item["CONTENT_ID"]
        version_id = version["CONTENT_VERSION_ID"]
        contexts.append(
            {
                "content_id": content_id,
                "content_item_key": content_item_key,
                "content_version_id": version_id,
                "item": item,
                "version": version,
                "bodies": [row for row in state.content_bodies.values() if row["CONTENT_ID"] == content_id and row["CONTENT_VERSION_ID"] == version_id],
                "assets": [row for row in state.content_assets.values() if row["CONTENT_ID"] == content_id and row["CONTENT_VERSION_ID"] == version_id],
                "scopes": [row for row in state.content_scopes.values() if row["CONTENT_ID"] == content_id and row["CONTENT_VERSION_ID"] == version_id],
                "entity_links": [row for row in state.content_entity_links.values() if row["CONTENT_ID"] == content_id and row["CONTENT_VERSION_ID"] == version_id],
                "dates": [row for row in state.content_dates.values() if row["CONTENT_ID"] == content_id and row["CONTENT_VERSION_ID"] == version_id],
            }
        )
    return contexts


def _derive_local_review_issues(current_contexts: list[dict[str, Any]]) -> list[ReviewIssue]:
    issues: list[ReviewIssue] = []
    for context in current_contexts:
        source_title = context["item"].get("SOURCE_TITLE") or context["item"]["CONTENT_ID"]
        bundle_id = (context["item"].get("METADATA") or {}).get("bundle_id")
        primary_body_count = sum(1 for body in context["bodies"] if body["IS_PRIMARY_BODY"])
        if primary_body_count == 0:
            issues.append(
                ReviewIssue(
                    content_id=context["content_id"],
                    content_version_id=context["content_version_id"],
                    issue_code="MISSING_PRIMARY_BODY",
                    severity="P1",
                    issue_detail="Current version has no primary body for document-level derivations.",
                    issue_context={"primary_body_count": primary_body_count},
                    source_title=source_title,
                    bundle_id=bundle_id,
                )
            )

        unresolved_mentions = [
            row for row in context["entity_links"] if row.get("ENTITY_ID") is None and row.get("ENTITY_NAME")
        ]
        if unresolved_mentions:
            issues.append(
                ReviewIssue(
                    content_id=context["content_id"],
                    content_version_id=context["content_version_id"],
                    issue_code="UNRESOLVED_ENTITY_MENTIONS",
                    severity="P2",
                    issue_detail="Entity mentions have names but no resolved entity ids.",
                    issue_context={"unresolved_entity_mention_count": len(unresolved_mentions)},
                    source_title=source_title,
                    bundle_id=bundle_id,
                )
            )

        missing_anchor_counts = _missing_anchor_counts(context)
        if missing_anchor_counts["investment"] > 0:
            issues.append(
                ReviewIssue(
                    content_id=context["content_id"],
                    content_version_id=context["content_version_id"],
                    issue_code="MISSING_INVESTMENT_BODY_ANCHORS",
                    severity="P1",
                    issue_detail="Multi-investment or mixed-scope content has approved investment links without body anchors.",
                    issue_context={"high_conf_investment_links_without_body": missing_anchor_counts["investment"]},
                    source_title=source_title,
                    bundle_id=bundle_id,
                )
            )
        if missing_anchor_counts["portfolio"] > 0:
            issues.append(
                ReviewIssue(
                    content_id=context["content_id"],
                    content_version_id=context["content_version_id"],
                    issue_code="MISSING_PORTFOLIO_BODY_ANCHORS",
                    severity="P2",
                    issue_detail="Mixed-scope content has approved portfolio/fund links without body anchors.",
                    issue_context={"high_conf_portfolio_links_without_body": missing_anchor_counts["portfolio"]},
                    source_title=source_title,
                    bundle_id=bundle_id,
                )
            )
        if missing_anchor_counts["strategy"] > 0:
            issues.append(
                ReviewIssue(
                    content_id=context["content_id"],
                    content_version_id=context["content_version_id"],
                    issue_code="MISSING_STRATEGY_BODY_ANCHORS",
                    severity="P2",
                    issue_detail="Mixed-scope content has approved strategy/topic links without body anchors.",
                    issue_context={"high_conf_strategy_links_without_body": missing_anchor_counts["strategy"]},
                    source_title=source_title,
                    bundle_id=bundle_id,
                )
            )

        if _select_operational_date(context) is None:
            issues.append(
                ReviewIssue(
                    content_id=context["content_id"],
                    content_version_id=context["content_version_id"],
                    issue_code="MISSING_OPERATIONAL_DATE",
                    severity="P3",
                    issue_detail="No selected operational date is available. Content remains valid, but timelines will show null effective dates.",
                    issue_context={"selected_operational_date_count": 0},
                    source_title=source_title,
                    bundle_id=bundle_id,
                )
            )
    return issues


def _missing_anchor_counts(context: dict[str, Any]) -> dict[str, int]:
    approved_investment = [
        row for row in context["entity_links"]
        if row["ENTITY_TYPE"] == "INVESTMENT"
        and row.get("ENTITY_NAME")
        and _is_review_approved(row)
        and _coalesce_float(row.get("CONFIDENCE")) >= 0.85
    ]
    approved_portfolio = [
        row for row in context["entity_links"]
        if row["ENTITY_TYPE"] in {"PORTFOLIO", "FUND"}
        and row.get("ENTITY_NAME")
        and _is_review_approved(row)
        and _coalesce_float(row.get("CONFIDENCE")) >= 0.80
    ]
    approved_strategy = [
        row for row in context["entity_links"]
        if row["ENTITY_TYPE"] in {"STRATEGY", "TOPIC"}
        and row.get("ENTITY_NAME")
        and _is_review_approved(row)
        and _coalesce_float(row.get("CONFIDENCE")) >= 0.80
    ]
    mixed_or_multi = _mixed_or_multi_scope(context["scopes"])
    return {
        "investment": sum(1 for row in approved_investment if not row.get("CONTENT_BODY_ID"))
        if (len(approved_investment) > 1 or mixed_or_multi)
        else 0,
        "portfolio": sum(1 for row in approved_portfolio if not row.get("CONTENT_BODY_ID"))
        if (len(approved_portfolio) > 1 or mixed_or_multi)
        else 0,
        "strategy": sum(1 for row in approved_strategy if not row.get("CONTENT_BODY_ID"))
        if (len(approved_strategy) > 1 or mixed_or_multi)
        else 0,
    }


def _derive_local_note_counts(current_contexts: list[dict[str, Any]]) -> dict[str, int]:
    derived_counts = {"investment": 0, "portfolio": 0, "strategy": 0}
    for context in current_contexts:
        mixed_or_multi = _mixed_or_multi_scope(context["scopes"])

        investment_links = [
            row for row in context["entity_links"]
            if row["ENTITY_TYPE"] == "INVESTMENT"
            and row.get("ENTITY_NAME")
            and _is_review_approved(row)
            and _coalesce_float(row.get("CONFIDENCE")) >= 0.85
        ]
        investment_anchor_required = len(investment_links) > 1 or mixed_or_multi
        derived_counts["investment"] += len(
            {
                (
                    row.get("ENTITY_ID") or normalize_entity_name(row.get("ENTITY_NAME")),
                    row.get("CONTENT_BODY_ID")
                    or (
                        "document"
                        if (
                            not investment_anchor_required
                            and (_is_primary_subject(row) or row.get("LINK_ROLE") in {"PRIMARY_SUBJECT", "DOCUMENT_SUBJECT"})
                        )
                        else None
                    ),
                )
                for row in investment_links
                if row.get("CONTENT_BODY_ID")
                or (
                    not investment_anchor_required
                    and (_is_primary_subject(row) or row.get("LINK_ROLE") in {"PRIMARY_SUBJECT", "DOCUMENT_SUBJECT"})
                )
            }
        )

        portfolio_links = [
            row for row in context["entity_links"]
            if row["ENTITY_TYPE"] in {"PORTFOLIO", "FUND"}
            and row.get("ENTITY_NAME")
            and _is_review_approved(row)
            and _coalesce_float(row.get("CONFIDENCE")) >= 0.80
        ]
        portfolio_anchor_required = len(portfolio_links) > 1 or mixed_or_multi
        derived_counts["portfolio"] += len(
            {
                (
                    row.get("ENTITY_ID") or normalize_entity_name(row.get("ENTITY_NAME")),
                    row.get("CONTENT_BODY_ID")
                    or (
                        "document"
                        if (
                            not portfolio_anchor_required
                            and (_is_primary_subject(row) or row.get("LINK_ROLE") in {"PRIMARY_SUBJECT", "DOCUMENT_SUBJECT"})
                        )
                        else None
                    ),
                )
                for row in portfolio_links
                if row.get("CONTENT_BODY_ID")
                or (
                    not portfolio_anchor_required
                    and (_is_primary_subject(row) or row.get("LINK_ROLE") in {"PRIMARY_SUBJECT", "DOCUMENT_SUBJECT"})
                )
            }
        )

        strategy_links = [
            row for row in context["entity_links"]
            if row["ENTITY_TYPE"] in {"STRATEGY", "TOPIC"}
            and row.get("ENTITY_NAME")
            and _is_review_approved(row)
            and _coalesce_float(row.get("CONFIDENCE")) >= 0.80
        ]
        strategy_anchor_required = len(strategy_links) > 1 or mixed_or_multi
        strategy_derived_count = len(
            {
                (
                    row.get("ENTITY_ID") or normalize_entity_name(row.get("ENTITY_NAME")),
                    row.get("CONTENT_BODY_ID")
                    or (
                        "document"
                        if (
                            not strategy_anchor_required
                            and (_is_primary_subject(row) or row.get("LINK_ROLE") in {"PRIMARY_SUBJECT", "DOCUMENT_SUBJECT"})
                        )
                        else None
                    ),
                )
                for row in strategy_links
                if row.get("CONTENT_BODY_ID")
                or (
                    not strategy_anchor_required
                    and (_is_primary_subject(row) or row.get("LINK_ROLE") in {"PRIMARY_SUBJECT", "DOCUMENT_SUBJECT"})
                )
            }
        )
        if strategy_derived_count == 0:
            strategy_scopes = [
                row for row in context["scopes"]
                if row["SCOPE_TYPE"] == "STRATEGY"
                and _coalesce_float(row.get("CONFIDENCE")) >= 0.80
                and str(row.get("REVIEW_STATUS", "")).upper() in {"AUTO_ACCEPTED", "APPROVED"}
                and (row.get("SCOPE_ENTITY_NAME") or row.get("SCOPE_LABEL"))
            ]
            strategy_derived_count = len(strategy_scopes)
        derived_counts["strategy"] += strategy_derived_count

    return derived_counts


def _compare_expected_entities(
    *,
    manifest: dict[str, Any] | None,
    current_contexts: list[dict[str, Any]],
) -> tuple[list[ValidationExample], list[ValidationExample], int]:
    if manifest is None:
        return [], [], 0

    expected_by_bundle: dict[str, list[dict[str, Any]]] = {}
    for bundle in manifest.get("bundles", []):
        expected_entities = list(bundle.get("expected", {}).get("entities", []) or [])
        if expected_entities:
            expected_by_bundle[bundle["bundle_id"]] = expected_entities

    actual_by_bundle: dict[str, list[dict[str, Any]]] = defaultdict(list)
    unresolved_by_bundle: dict[str, list[dict[str, Any]]] = defaultdict(list)
    source_title_by_bundle: dict[str, str] = {}
    seen_actual: set[tuple[str, str, str]] = set()
    seen_unresolved: set[tuple[str, str, str]] = set()
    for context in current_contexts:
        bundle_id = ((context["item"].get("METADATA") or {}).get("bundle_id"))
        if bundle_id is None or bundle_id not in expected_by_bundle:
            continue
        source_title = context["item"].get("SOURCE_TITLE") or context["content_id"]
        source_title_by_bundle[bundle_id] = source_title
        for row in context["entity_links"]:
            normalized = {
                "entity_type": row.get("ENTITY_TYPE"),
                "entity_name": normalize_entity_name(row.get("ENTITY_NAME")),
                "entity_id": row.get("ENTITY_ID"),
                "review_status": row.get("REVIEW_STATUS"),
                "confidence": row.get("CONFIDENCE"),
            }
            if row.get("ENTITY_ID"):
                dedupe_key = (bundle_id, str(normalized["entity_type"]).upper(), normalized["entity_name"] or "")
                if dedupe_key not in seen_actual:
                    seen_actual.add(dedupe_key)
                    actual_by_bundle[bundle_id].append(normalized)
            elif row.get("ENTITY_NAME"):
                dedupe_key = (bundle_id, str(normalized["entity_type"]).upper(), normalized["entity_name"] or "")
                if dedupe_key not in seen_unresolved:
                    seen_unresolved.add(dedupe_key)
                    unresolved_by_bundle[bundle_id].append(normalized)

    false_positives: list[ValidationExample] = []
    false_negatives: list[ValidationExample] = []
    for bundle_id, expected_entities in expected_by_bundle.items():
        expected_keys = {
            (
                str(entity.get("entity_type", "")).upper(),
                normalize_entity_name(entity.get("entity_name")),
            )
            for entity in expected_entities
            if entity.get("entity_type") and entity.get("entity_name")
        }
        actual_resolved_keys = {
            (str(entity["entity_type"]).upper(), entity["entity_name"])
            for entity in actual_by_bundle.get(bundle_id, [])
            if entity["entity_name"]
        }
        unresolved_keys = {
            (str(entity["entity_type"]).upper(), entity["entity_name"])
            for entity in unresolved_by_bundle.get(bundle_id, [])
            if entity["entity_name"]
        }
        source_title = source_title_by_bundle.get(bundle_id, bundle_id)

        for actual in actual_by_bundle.get(bundle_id, []):
            actual_key = (str(actual["entity_type"]).upper(), actual["entity_name"])
            if actual_key not in expected_keys:
                false_positives.append(
                    ValidationExample(
                        source_title=source_title,
                        bundle_id=bundle_id,
                        entity_type=actual_key[0],
                        expected_entity=None,
                        actual_entity=actual_key[1],
                        reason="Resolved entity link is not present in the labeled expected set for this bundle.",
                    )
                )

        for expected in expected_entities:
            expected_key = (
                str(expected.get("entity_type", "")).upper(),
                normalize_entity_name(expected.get("entity_name")),
            )
            if expected_key in actual_resolved_keys:
                continue
            reason = "Expected entity is missing from resolved entity links."
            if expected_key in unresolved_keys:
                reason = "Expected entity appears only as an unresolved mention and did not resolve to an entity id."
            false_negatives.append(
                ValidationExample(
                    source_title=source_title,
                    bundle_id=bundle_id,
                    entity_type=expected_key[0],
                    expected_entity=expected_key[1],
                    actual_entity=None,
                    reason=reason,
                )
            )

    return false_positives, false_negatives, len(expected_by_bundle)


def _select_operational_date(context: dict[str, Any]) -> dict[str, Any] | None:
    candidates: list[dict[str, Any]] = []
    for row in context["dates"]:
        if row["DATE_TYPE"] not in ALLOWED_OPERATIONAL_DATE_TYPES:
            continue
        operational_date = (
            row.get("DATE_VALUE")
            or _date_from_timestamp(row.get("TIMESTAMP_VALUE"))
            or row.get("PERIOD_END_DATE")
            or row.get("PERIOD_START_DATE")
        )
        if operational_date is None:
            continue
        candidates.append(
            {
                "row": row,
                "operational_date": operational_date,
                "priority": DATE_PRIORITY.get(row["DATE_TYPE"], 90),
            }
        )
    if not candidates:
        return None
    candidates.sort(
        key=lambda candidate: (
            0 if candidate["row"].get("CONTENT_VERSION_ID") else 1,
            0 if candidate["row"].get("IS_PRIMARY_CANDIDATE") else 1,
            candidate["priority"],
            -_coalesce_float(candidate["row"].get("CONFIDENCE")),
            str(candidate["operational_date"]),
            str(candidate["row"].get("STAGED_AT") or ""),
            candidate["row"]["CONTENT_DATE_ID"],
        )
    )
    return candidates[-1]


def _build_blocked_notes(issues: list[ReviewIssue]) -> list[BlockedNote]:
    grouped: dict[str, list[ReviewIssue]] = defaultdict(list)
    for issue in issues:
        if issue.severity == "P1":
            grouped[issue.content_id].append(issue)
    blocked_notes: list[BlockedNote] = []
    for content_id, grouped_issues in grouped.items():
        source_title = grouped_issues[0].source_title or content_id
        blocked_notes.append(
            BlockedNote(
                content_id=content_id,
                source_title=source_title,
                issue_codes=tuple(sorted({issue.issue_code for issue in grouped_issues})),
                severity="P1",
                issue_details=tuple(issue.issue_detail for issue in grouped_issues),
            )
        )
    blocked_notes.sort(key=lambda note: note.source_title.lower())
    return blocked_notes


def _issue_examples(issues: list[ReviewIssue], issue_codes: set[str]) -> list[str]:
    filtered = [issue for issue in issues if issue.issue_code in issue_codes]
    if not filtered:
        return ["- None"]
    return [
        f"- `{issue.issue_code}` on `{issue.source_title or issue.content_id}`: {issue.issue_detail}"
        for issue in filtered[:10]
    ]


def _example_lines(examples: list[ValidationExample]) -> list[str]:
    if not examples:
        return ["- None"]
    return [
        f"- `{example.source_title}` [{example.entity_type or 'UNKNOWN'}]: expected `{example.expected_entity or 'n/a'}`, "
        f"actual `{example.actual_entity or 'n/a'}`. {example.reason}"
        for example in examples[:10]
    ]


def _blocked_note_lines(blocked_notes: list[BlockedNote]) -> list[str]:
    if not blocked_notes:
        return ["- None"]
    return [
        f"- `{note.source_title}`: blocked by `{', '.join(note.issue_codes)}`"
        for note in blocked_notes
    ]


def _mixed_or_multi_scope(scopes: list[dict[str, Any]]) -> bool:
    return any(scope["SCOPE_TYPE"] in {"MIXED", "MULTI_INVESTMENT"} for scope in scopes)


def _is_review_approved(row: dict[str, Any]) -> bool:
    return str(row.get("REVIEW_STATUS", "")).upper() in {"AUTO_ACCEPTED", "APPROVED"}


def _is_primary_subject(row: dict[str, Any]) -> bool:
    return bool(row.get("IS_PRIMARY_SUBJECT"))


def _coalesce_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _date_from_timestamp(value: Any) -> str | None:
    if value is None:
        return None
    return str(value).split("T", 1)[0]


def _utc_now_isoformat() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
