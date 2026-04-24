"""Scope classification for parsed APS markdown notes."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from .entity_linker import EntityLinkCandidate
from .normalize import normalize_scope_type


@dataclass(frozen=True)
class ScopeCandidate:
    scope_type: str
    scope_label: str | None
    scope_entity_type: str | None
    scope_entity_id: str | None
    scope_entity_name: str | None
    is_primary_scope: bool
    confidence: float
    evidence_text: str
    review_status: str
    metadata: dict[str, Any] = field(default_factory=dict)


def classify_scopes(parsed_document: Any, entity_links: list[EntityLinkCandidate]) -> list[ScopeCandidate]:
    resolved = [link for link in entity_links if link.entity_id]
    by_type: dict[str, list[EntityLinkCandidate]] = {}
    for link in resolved:
        by_type.setdefault(link.entity_type, []).append(link)

    scopes: list[ScopeCandidate] = []
    investment_links = _unique_links(by_type.get("INVESTMENT", []))
    portfolio_links = _unique_links(by_type.get("PORTFOLIO", []))
    strategy_links = _unique_links(by_type.get("STRATEGY", []))

    has_mixed = sum(bool(group) for group in (investment_links, portfolio_links, strategy_links)) > 1
    if has_mixed:
        scopes.append(
            ScopeCandidate(
                scope_type="MIXED",
                scope_label=parsed_document.title,
                scope_entity_type=None,
                scope_entity_id=None,
                scope_entity_name=None,
                is_primary_scope=True,
                confidence=0.9,
                evidence_text="Resolved entities span more than one scope class.",
                review_status="AUTO_ACCEPTED",
            )
        )
    elif len(investment_links) > 1:
        scopes.append(
            ScopeCandidate(
                scope_type="MULTI_INVESTMENT",
                scope_label=parsed_document.title,
                scope_entity_type=None,
                scope_entity_id=None,
                scope_entity_name=None,
                is_primary_scope=True,
                confidence=0.95,
                evidence_text="More than one resolved investment entity is linked to the note.",
                review_status="AUTO_ACCEPTED",
            )
        )
    elif investment_links:
        scopes.append(_scope_from_link("INVESTMENT", investment_links[0], is_primary_scope=True))
    elif portfolio_links:
        scopes.append(_scope_from_link("PORTFOLIO", portfolio_links[0], is_primary_scope=True))
    elif strategy_links:
        scopes.append(_scope_from_link("STRATEGY", strategy_links[0], is_primary_scope=True))
    else:
        scopes.append(
            ScopeCandidate(
                scope_type=normalize_scope_type(_fallback_scope_from_title(parsed_document.title)) or "UNKNOWN",
                scope_label=parsed_document.title,
                scope_entity_type=None,
                scope_entity_id=None,
                scope_entity_name=None,
                is_primary_scope=True,
                confidence=0.3,
                evidence_text="No resolved entity links were available; scope inferred only from title heuristics.",
                review_status="NEEDS_REVIEW",
            )
        )

    if scopes[0].scope_type in {"MULTI_INVESTMENT", "MIXED"}:
        for link in investment_links:
            scopes.append(_scope_from_link("INVESTMENT", link, is_primary_scope=False))
        for link in portfolio_links:
            scopes.append(_scope_from_link("PORTFOLIO", link, is_primary_scope=False))
        for link in strategy_links:
            scopes.append(_scope_from_link("STRATEGY", link, is_primary_scope=False))

    return scopes


def _unique_links(links: list[EntityLinkCandidate]) -> list[EntityLinkCandidate]:
    seen: set[tuple[str | None, str]] = set()
    unique: list[EntityLinkCandidate] = []
    for link in sorted(links, key=lambda value: (-value.confidence, value.entity_name.lower())):
        identity = (link.entity_id, link.entity_name.lower())
        if identity in seen:
            continue
        seen.add(identity)
        unique.append(link)
    return unique


def _scope_from_link(scope_type: str, link: EntityLinkCandidate, *, is_primary_scope: bool) -> ScopeCandidate:
    return ScopeCandidate(
        scope_type=scope_type,
        scope_label=link.entity_name,
        scope_entity_type=scope_type,
        scope_entity_id=link.entity_id,
        scope_entity_name=link.entity_name,
        is_primary_scope=is_primary_scope,
        confidence=link.confidence,
        evidence_text="Derived directly from a resolved entity link.",
        review_status=link.review_status,
        metadata={"content_body_id": link.content_body_id},
    )


def _fallback_scope_from_title(title: str) -> str:
    lowered = title.lower()
    if "portfolio" in lowered or "fund" in lowered:
        return "PORTFOLIO"
    if "weekly" in lowered or "monthly" in lowered or "strategy" in lowered:
        return "STRATEGY"
    return "UNKNOWN"

