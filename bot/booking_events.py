from __future__ import annotations

from typing import Any, Dict, Optional

from sqlalchemy.ext.asyncio import AsyncSession

try:
    # Когда модуль используется внутри пакета bot (админка)
    from bot.db import BookingEvent, BookingEventType, User  # type: ignore
except ImportError:
    # Когда запускается из бота как корневого модуля
    from db import BookingEvent, BookingEventType, User  # type: ignore


async def log_booking_event(
    session: AsyncSession,
    user: User,
    event_type: BookingEventType,
    yclients_record_id: Optional[int] = None,
    meta: Optional[Dict[str, Any]] = None,
) -> None:
    """
    Универсальный логгер событий, связанных с записями.
    Пишет в таблицу booking_events, которую использует админская аналитика.

    ВАЖНО:
    - В модели BookingEvent НЕТ отдельного поля yclients_record_id,
      поэтому сохраняем его внутрь meta как "record_id".
    """
    meta_combined: Dict[str, Any] = dict(meta or {})
    if yclients_record_id is not None:
        # Сохраняем ID записи как часть meta
        meta_combined.setdefault("record_id", yclients_record_id)

    # Если логируем отмену — удалим возможный COMPLETED с тем же record_id,
    # чтобы в аналитике не было одновременно выполнено+отменено.
    if event_type == BookingEventType.CANCELLED and meta_combined.get("record_id"):
        await session.execute(
            BookingEvent.__table__.delete().where(
                BookingEvent.event_type == BookingEventType.COMPLETED,
                BookingEvent.meta["record_id"].astext == str(meta_combined["record_id"]),
            )
        )
        await session.commit()

    event = BookingEvent(
        user_id=user.id,
        event_type=event_type,  # поле в модели называется event_type
        meta=meta_combined,
    )
    session.add(event)
    await session.commit()

