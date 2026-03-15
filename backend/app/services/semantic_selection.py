from __future__ import annotations

import re
from typing import NamedTuple
from uuid import UUID

from sqlalchemy import and_, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Action, Capability


class SelectedCapability(NamedTuple):
    capability: Capability
    score: float
    confidence_tier: str = "high"


class SemanticSelectionService:
    HIGH_CONFIDENCE_THRESHOLD = 0.45
    MEDIUM_CONFIDENCE_THRESHOLD = 0.30
    LOW_MARGIN_THRESHOLD = 0.05
    CRM_TOKENS = {
        "crm",
        "segment",
        "segments",
        "audience",
        "campaign",
        "campaigns",
        "mailing",
        "newsletter",
        "lead",
        "leads",
        "retention",
        "cohort",
        "churn",
        "conversion",
        "promo",
        "offer",
        "offers",
        "email",
        "emails",
        "push",
        "sale",
        "sales",
        "сегмент",
        "сегменты",
        "аудитория",
        "кампания",
        "кампании",
        "рассылка",
        "лид",
        "лиды",
        "ретеншн",
        "конверсия",
        "оффер",
        "офферы",
        "пуш",
        "продажи",
        "клиент",
        "клиенты",
    }
    GENERIC_TOKENS = {
        "get",
        "list",
        "create",
        "update",
        "delete",
        "call",
        "data",
        "info",
        "items",
        "resource",
        "resources",
        "service",
        "api",
        "handle",
        "handler",
        "manage",
        "process",
        "method",
        "action",
        "fetch",
        "general",
        "common",
        "получить",
        "список",
        "создать",
        "обновить",
        "удалить",
        "данные",
        "инфо",
        "ресурс",
        "сервис",
        "метод",
        "действие",
        "общее",
    }
    _STOPWORDS = {
        "and",
        "the",
        "for",
        "with",
        "from",
        "into",
        "that",
        "this",
        "что",
        "это",
        "как",
        "для",
        "или",
        "при",
        "про",
        "надо",
        "нужно",
        "хочу",
        "build",
        "pipeline",
        "workflow",
        "scenario",
        "automation",
        "пайплайн",
        "сценарий",
        "автоматизация",
        "построй",
        "собери",
    }
    _ALIAS_EXPANSIONS = {
        "польз": {"user", "users", "client", "clients", "пользователь", "пользователи"},
        "клиент": {"client", "clients", "user", "users", "клиент", "клиенты"},
        "юзер": {"user", "users", "пользователь", "пользователи"},
        "получ": {"get", "fetch", "list", "retrieve", "получить", "список"},
        "спис": {"list", "get", "fetch", "список", "получить"},
        "созд": {"create", "add", "post", "создать"},
        "обнов": {"update", "patch", "put", "обновить"},
        "удал": {"delete", "remove", "del", "удалить"},
        "рассыл": {"mailing", "newsletter", "broadcast", "email", "рассылка"},
        "сегмент": {"segment", "segments", "сегмент", "сегменты"},
        "лид": {"lead", "leads", "лид", "лиды"},
        "отчет": {"report", "analytics", "отчет", "отчёт"},
        "отчёт": {"report", "analytics", "отчет", "отчёт"},
        "user": {"пользователь", "пользователи", "user", "users"},
        "users": {"пользователь", "пользователи", "user", "users"},
        "get": {"получить", "список", "get", "fetch", "list"},
        "fetch": {"получить", "список", "get", "fetch", "list"},
        "list": {"получить", "список", "get", "fetch", "list"},
    }

    async def select_capabilities(
        self,
        session: AsyncSession,
        user_query: str,
        owner_user_id: UUID | None = None,
        limit: int = 10,
    ) -> list[SelectedCapability]:
        query_tokens = self._tokenize(user_query)
        if not query_tokens:
            return []

        query = select(Capability).order_by(Capability.created_at.asc())
        if owner_user_id is not None:
            # User-scoped with legacy compatibility:
            # some old capabilities may have user_id=NULL while their source action has owner.
            query = query.outerjoin(Action, Capability.action_id == Action.id).where(
                or_(
                    Capability.user_id == owner_user_id,
                    and_(
                        Capability.user_id.is_(None),
                        Action.user_id == owner_user_id,
                    ),
                )
            )
        query = query.limit(200)
        result = await session.execute(query)
        capabilities = list(result.scalars().all())

        executable_capabilities = [
            capability
            for capability in capabilities
            if getattr(capability, "action_id", None) is not None
        ]
        candidates = executable_capabilities
        if not candidates:
            return []

        query_tokens_expanded = self._expand_tokens(query_tokens)
        ranked: list[SelectedCapability] = []
        for capability in candidates:
            score = self._score_capability(query_tokens, query_tokens_expanded, capability)
            if score <= 0:
                continue
            ranked.append(SelectedCapability(capability=capability, score=score))

        ranked.sort(key=lambda item: item.score, reverse=True)
        if not ranked:
            if candidates:
                # Fallback: keep generation moving even when lexical matching is weak.
                return [
                    SelectedCapability(
                        capability=capability,
                        score=0.01,
                        confidence_tier="low",
                    )
                    for capability in candidates[:limit]
                ]
            return []

        top_score = ranked[0].score
        second_score = ranked[1].score if len(ranked) > 1 else 0.0
        margin = top_score - second_score
        confidence_tier = self._resolve_confidence_tier(top_score, margin)

        return [
            SelectedCapability(
                capability=item.capability,
                score=item.score,
                confidence_tier=confidence_tier,
            )
            for item in ranked[:limit]
        ]

    def _score_capability(
        self,
        query_tokens: set[str],
        query_tokens_expanded: set[str],
        capability: Capability,
    ) -> float:
        name = str(getattr(capability, "name", "") or "")
        description = str(getattr(capability, "description", "") or "")
        name_tokens = self._tokenize(name)
        description_tokens = self._tokenize(description)
        combined_tokens = name_tokens | description_tokens
        if not combined_tokens:
            return 0.0

        combined_tokens_expanded = self._expand_tokens(combined_tokens)
        overlap = query_tokens_expanded & combined_tokens_expanded
        if not overlap:
            return 0.0

        overlap_ratio = len(overlap) / len(query_tokens_expanded)
        name_tokens_expanded = self._expand_tokens(name_tokens)
        name_ratio = len(query_tokens_expanded & name_tokens_expanded) / len(query_tokens_expanded)
        exact_bonus = 0.22 if query_tokens_expanded <= combined_tokens_expanded else 0.0

        generic_expanded = self._expand_tokens(self.GENERIC_TOKENS)
        entity_overlap = overlap - generic_expanded
        entity_bonus = min(0.18, len(entity_overlap) * 0.06) if entity_overlap else 0.0

        query_crm_tokens = query_tokens_expanded & self.CRM_TOKENS
        capability_crm_tokens = combined_tokens_expanded & self.CRM_TOKENS
        crm_bonus = 0.0
        if query_crm_tokens and capability_crm_tokens:
            crm_overlap = len(query_crm_tokens & capability_crm_tokens)
            crm_bonus = 0.12 + min(0.14, crm_overlap * 0.04)

        generic_penalty = self._generic_capability_penalty(combined_tokens)

        return (
            max(overlap_ratio, name_ratio * 1.12)
            + exact_bonus
            + entity_bonus
            + crm_bonus
            - generic_penalty
        )

    def _resolve_confidence_tier(self, top_score: float, margin: float) -> str:
        if margin < self.LOW_MARGIN_THRESHOLD:
            return "low"
        if top_score >= self.HIGH_CONFIDENCE_THRESHOLD:
            return "high"
        if top_score >= self.MEDIUM_CONFIDENCE_THRESHOLD:
            return "medium"
        return "low"

    def _generic_capability_penalty(self, tokens: set[str]) -> float:
        if not tokens:
            return 0.0
        generic_share = len(tokens & self.GENERIC_TOKENS) / len(tokens)
        if generic_share >= 0.65:
            return 0.14
        if generic_share >= 0.5:
            return 0.09
        if generic_share >= 0.35:
            return 0.04
        return 0.0

    def _tokenize(self, value: str) -> set[str]:
        tokens = set(re.findall(r"[a-zA-Zа-яА-Я0-9]+", value.lower()))
        return {
            token
            for token in tokens
            if len(token) >= 3 and token not in self._STOPWORDS
        }

    def _expand_tokens(self, tokens: set[str]) -> set[str]:
        expanded: set[str] = set()
        for token in tokens:
            expanded.add(token)
            normalized_variants = self._normalized_variants(token)
            expanded.update(normalized_variants)
            for variant in normalized_variants | {token}:
                for key, aliases in self._ALIAS_EXPANSIONS.items():
                    if variant == key or variant.startswith(key):
                        expanded.update(aliases)
        return expanded

    def _normalized_variants(self, token: str) -> set[str]:
        variants = {token}
        if len(token) >= 5:
            for suffix in (
                "иями",
                "ями",
                "ами",
                "ов",
                "ев",
                "ей",
                "ам",
                "ям",
                "ах",
                "ях",
                "ые",
                "ий",
                "ый",
                "ая",
                "ое",
                "ой",
                "а",
                "я",
                "ы",
                "и",
                "у",
                "ю",
                "е",
                "о",
            ):
                if token.endswith(suffix) and len(token) > len(suffix) + 2:
                    variants.add(token[: -len(suffix)])

        if token.endswith("ies") and len(token) > 4:
            variants.add(token[:-3] + "y")
        if token.endswith("s") and len(token) > 3:
            variants.add(token[:-1])
        return variants
