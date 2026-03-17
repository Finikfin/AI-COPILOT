from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Annotated

from fastapi import FastAPI, Query
from pydantic import BaseModel, Field


class User(BaseModel):
    id: str
    email: str
    last_active: datetime


class Hotel(BaseModel):
    id: str
    name: str
    city: str


class Segment(BaseModel):
    segment_id: str
    hotel_id: str
    user_ids: list[str] = Field(default_factory=list)


class Assignment(BaseModel):
    user_id: str
    hotel_id: str


class RecentUsersResponse(BaseModel):
    users: list[User] = Field(default_factory=list)


class TopHotelsResponse(BaseModel):
    hotels: list[Hotel] = Field(default_factory=list)


class HotelSegmentsRequest(BaseModel):
    users: list[User] = Field(default_factory=list)
    hotels: list[Hotel] = Field(default_factory=list)


class HotelSegmentsResponse(BaseModel):
    segments: list[Segment] = Field(default_factory=list)


class AssignmentsRequest(BaseModel):
    segments: list[Segment] = Field(default_factory=list)


class AssignmentsResponse(BaseModel):
    assignments: list[Assignment] = Field(default_factory=list)


class EmailOfferRequest(BaseModel):
    template_id: str
    assignments: list[Assignment] = Field(default_factory=list)


class FailedDelivery(BaseModel):
    user_id: str
    reason: str


class EmailOfferResponse(BaseModel):
    sent_count: int
    failed_count: int
    failed: list[FailedDelivery] = Field(default_factory=list)


class Lead(BaseModel):
    lead_id: str
    email: str
    source: str


class QualifiedLead(BaseModel):
    lead_id: str
    email: str
    score: int
    tier: str


class PreparedOffer(BaseModel):
    offer_id: str
    lead_id: str
    channel: str
    message: str


class RecentLeadsResponse(BaseModel):
    leads: list[Lead] = Field(default_factory=list)


class QualifyLeadsRequest(BaseModel):
    leads: list[Lead] = Field(default_factory=list)


class QualifyLeadsResponse(BaseModel):
    qualified_leads: list[QualifiedLead] = Field(default_factory=list)


class PrepareOffersRequest(BaseModel):
    qualified_leads: list[QualifiedLead] = Field(default_factory=list)


class PrepareOffersResponse(BaseModel):
    offers: list[PreparedOffer] = Field(default_factory=list)


class SendOffersRequest(BaseModel):
    offers: list[PreparedOffer] = Field(default_factory=list)


class FailedLeadDelivery(BaseModel):
    lead_id: str
    reason: str


class SendOffersResponse(BaseModel):
    sent_count: int
    failed_count: int
    failed: list[FailedLeadDelivery] = Field(default_factory=list)


APP_DESCRIPTION = """
Synthetic API with multiple linear demo workflows.

Travel workflow:
1. `GET /users/recent`
2. `GET /hotels/top`
3. `POST /segments/hotel`
4. `POST /assignments/hotels`
5. `POST /emails/send-offers`

CRM workflow:
1. `GET /crm/leads/recent`
2. `POST /crm/leads/qualify`
3. `POST /crm/offers/prepare`
4. `POST /crm/offers/send`
""".strip()


app = FastAPI(
    title="Travel Product Manager API",
    version="1.0.0",
    description=APP_DESCRIPTION,
)


BASE_USERS_TS = datetime(2026, 3, 13, 10, 0, tzinfo=timezone.utc)
HOTEL_CATALOG: list[Hotel] = [
    Hotel(id="hotel_001", name="Hotel Aurora", city="Berlin"),
    Hotel(id="hotel_002", name="Sea Breeze Resort", city="Lisbon"),
    Hotel(id="hotel_003", name="Mountain Vista", city="Zurich"),
    Hotel(id="hotel_004", name="City Loft", city="Amsterdam"),
    Hotel(id="hotel_005", name="River Palace", city="Prague"),
    Hotel(id="hotel_006", name="Nordic Harbor", city="Stockholm"),
    Hotel(id="hotel_007", name="Sunset Bay", city="Barcelona"),
    Hotel(id="hotel_008", name="Alpine Crown", city="Vienna"),
]


def _build_users() -> list[User]:
    users: list[User] = []
    for idx in range(1, 31):
        users.append(
            User(
                id=f"usr_{idx:03d}",
                email=f"user{idx:03d}@example.com",
                last_active=BASE_USERS_TS - timedelta(minutes=(idx - 1) * 5),
            )
        )
    return users


def _build_recent_leads() -> list[Lead]:
    leads: list[Lead] = []
    sources = ["landing", "webinar", "partner", "organic"]
    for idx in range(1, 21):
        leads.append(
            Lead(
                lead_id=f"lead_{idx:03d}",
                email=f"lead{idx:03d}@example.com",
                source=sources[idx % len(sources)],
            )
        )
    return leads


@app.get(
    "/users/recent",
    response_model=RecentUsersResponse,
    operation_id="getRecentUsers",
    tags=["travel-offer-workflow"],
)
async def get_recent_users(
    last_active_after: Annotated[datetime | None, Query()] = None,
    limit: Annotated[int, Query(ge=1, le=100)] = 30,
) -> RecentUsersResponse:
    users = _build_users()
    if last_active_after is not None:
        users = [user for user in users if user.last_active > last_active_after]
    return RecentUsersResponse(users=users[:limit])


@app.get(
    "/hotels/top",
    response_model=TopHotelsResponse,
    operation_id="getTopHotels",
    tags=["travel-offer-workflow"],
)
async def get_top_hotels(
    limit: Annotated[int, Query(ge=1, le=20)] = 5,
    city: Annotated[str | None, Query()] = None,
) -> TopHotelsResponse:
    hotels = HOTEL_CATALOG
    if city:
        city_normalized = city.strip().lower()
        hotels = [hotel for hotel in hotels if hotel.city.lower() == city_normalized]
    return TopHotelsResponse(hotels=hotels[:limit])


@app.post(
    "/segments/hotel",
    response_model=HotelSegmentsResponse,
    operation_id="segmentUsersByHotelPreferences",
    tags=["travel-offer-workflow"],
)
async def segment_users_by_hotel_preferences(
    payload: HotelSegmentsRequest,
) -> HotelSegmentsResponse:
    if not payload.users or not payload.hotels:
        return HotelSegmentsResponse(segments=[])

    grouped: dict[str, list[str]] = {hotel.id: [] for hotel in payload.hotels}
    for index, user in enumerate(payload.users):
        hotel = payload.hotels[index % len(payload.hotels)]
        grouped[hotel.id].append(user.id)

    segments: list[Segment] = []
    for hotel in payload.hotels:
        user_ids = grouped.get(hotel.id, [])
        if not user_ids:
            continue
        segments.append(
            Segment(
                segment_id=f"seg_{hotel.id}",
                hotel_id=hotel.id,
                user_ids=user_ids,
            )
        )

    return HotelSegmentsResponse(segments=segments)


@app.post(
    "/assignments/hotels",
    response_model=AssignmentsResponse,
    operation_id="assignUsersToHotels",
    tags=["travel-offer-workflow"],
)
async def assign_users_to_hotels(payload: AssignmentsRequest) -> AssignmentsResponse:
    assignments: list[Assignment] = []
    for segment in payload.segments:
        for user_id in segment.user_ids:
            assignments.append(Assignment(user_id=user_id, hotel_id=segment.hotel_id))
    return AssignmentsResponse(assignments=assignments)


@app.post(
    "/emails/send-offers",
    response_model=EmailOfferResponse,
    status_code=200,
    operation_id="sendHotelOffersByEmail",
    tags=["travel-offer-workflow"],
)
async def send_hotel_offers_by_email(payload: EmailOfferRequest) -> EmailOfferResponse:
    _ = payload.template_id

    failed: list[FailedDelivery] = []
    for assignment in payload.assignments:
        if assignment.user_id.endswith("000"):
            failed.append(
                FailedDelivery(
                    user_id=assignment.user_id,
                    reason="Invalid user id for delivery",
                )
            )

    sent_count = len(payload.assignments) - len(failed)
    return EmailOfferResponse(
        sent_count=sent_count,
        failed_count=len(failed),
        failed=failed,
    )


@app.get(
    "/crm/leads/recent",
    response_model=RecentLeadsResponse,
    operation_id="getRecentLeads",
    tags=["crm-linear-workflow"],
)
async def get_recent_leads(
    limit: Annotated[int, Query(ge=1, le=50)] = 20,
    source: Annotated[str | None, Query()] = None,
) -> RecentLeadsResponse:
    leads = _build_recent_leads()
    if source:
        source_normalized = source.strip().lower()
        leads = [lead for lead in leads if lead.source.lower() == source_normalized]
    return RecentLeadsResponse(leads=leads[:limit])


@app.post(
    "/crm/leads/qualify",
    response_model=QualifyLeadsResponse,
    operation_id="qualifyLeadsForOffer",
    tags=["crm-linear-workflow"],
)
async def qualify_leads_for_offer(payload: QualifyLeadsRequest) -> QualifyLeadsResponse:
    qualified: list[QualifiedLead] = []
    for index, lead in enumerate(payload.leads):
        score = 55 + ((index * 7) % 45)
        tier = "high" if score >= 80 else "medium" if score >= 65 else "low"
        qualified.append(
            QualifiedLead(
                lead_id=lead.lead_id,
                email=lead.email,
                score=score,
                tier=tier,
            )
        )
    return QualifyLeadsResponse(qualified_leads=qualified)


@app.post(
    "/crm/offers/prepare",
    response_model=PrepareOffersResponse,
    operation_id="prepareOffersForLeads",
    tags=["crm-linear-workflow"],
)
async def prepare_offers_for_leads(payload: PrepareOffersRequest) -> PrepareOffersResponse:
    offers: list[PreparedOffer] = []
    for lead in payload.qualified_leads:
        channel = "email" if lead.tier in {"high", "medium"} else "push"
        offers.append(
            PreparedOffer(
                offer_id=f"offer_{lead.lead_id}",
                lead_id=lead.lead_id,
                channel=channel,
                message=f"Special travel offer for {lead.tier} intent lead",
            )
        )
    return PrepareOffersResponse(offers=offers)


@app.post(
    "/crm/offers/send",
    response_model=SendOffersResponse,
    operation_id="sendPreparedOffers",
    tags=["crm-linear-workflow"],
)
async def send_prepared_offers(payload: SendOffersRequest) -> SendOffersResponse:
    failed: list[FailedLeadDelivery] = []
    for offer in payload.offers:
        if offer.lead_id.endswith("000"):
            failed.append(
                FailedLeadDelivery(
                    lead_id=offer.lead_id,
                    reason="Invalid lead for delivery",
                )
            )

    sent_count = len(payload.offers) - len(failed)
    return SendOffersResponse(
        sent_count=sent_count,
        failed_count=len(failed),
        failed=failed,
    )


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}
