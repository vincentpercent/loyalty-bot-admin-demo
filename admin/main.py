import os
import json
import secrets
import logging
import re
import asyncio
from datetime import datetime, timedelta, date, timezone
from pathlib import Path
from types import SimpleNamespace  # для заглушечных stats по записям

from fastapi import FastAPI, Request, Depends, HTTPException, Form
from fastapi.responses import HTMLResponse, RedirectResponse, StreamingResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from sqlalchemy import select, func, cast, String, case, delete
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from aiogram import Bot

from bot.db import (
    AsyncSessionLocal,
    User,
    UserBonus,
    get_or_create_user_bonus,
    get_bonus_config,
    BotText,
    DEFAULT_BOT_TEXTS,
    init_bot_texts,
    Promocode,
    PromocodeUsage,
    sync_bonus_to_yclients,
    sync_bonus_from_yclients,
    get_user_by_telegram_id,
    get_user_by_yclients_id,
    log_button_click,
)
from admin.models import (
    ReviewBonusRequest,
    ReviewRequestStatus,
    BonusTransaction,
    BonusTransactionType,
    BonusTransactionSource,
    AuditLog,
    BookingEventType,
    BookingEvent,
    ButtonEvent,
)
from config import settings  # пока просто лежит, пригодится для других настроек
from bot.yclients_client import YClientsClient
from bot.booking_events import log_booking_event


logger = logging.getLogger(__name__)

app = FastAPI(title="Loyalty Demo Admin")


@app.get("/health", tags=["health"])
async def health_check():
    return {"status": "ok"}


@app.get("/booking_redirect", include_in_schema=False)
async def booking_redirect(tg_id: int | None = None):
    # Пробуем пронести метку source=bot в форму записи, чтобы различать записи из бота
    target = settings.BOOKING_URL
    if not target:
        raise HTTPException(status_code=500, detail="BOOKING_URL is not configured")

    try:
        async with AsyncSessionLocal() as session:
            user = None
            if tg_id:
                user = await get_user_by_telegram_id(session, tg_id)

            # Лог клика (кнопки)
            await log_button_click(
                session=session,
                user_id=user.id if user else None,
                button_name="💈 Записаться онлайн",
            )

            # Лог события booking click, если знаем пользователя
            if user:
                await log_booking_event(
                    session=session,
                    user=user,
                    event_type=BookingEventType.CLICK_BOOKING,
                    yclients_record_id=None,
                    meta={"source": "inline_redirect"},
                )
    except Exception:
        logger.exception("Failed to log booking_redirect", extra={"tg_id": tg_id})

    # Добавляем метку source=bot (и tg_id если есть) в URL YClients.
    # Если форма не принимает query-параметры, это просто безвредный хвост.
    from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

    parsed = urlparse(target)
    q = parse_qs(parsed.query)
    q["source"] = ["bot"]
    if tg_id:
        q["tg_id"] = [str(tg_id)]
    new_query = urlencode(q, doseq=True)
    target_with_tag = urlunparse(parsed._replace(query=new_query))

    return RedirectResponse(target_with_tag)


# =========================
#   SYNC ЗАПИСЕЙ YCLIENTS (polling вместо вебхука)
# =========================

YCLIENTS_SYNC_LOOKBACK_DAYS = int(os.getenv("YCLIENTS_SYNC_LOOKBACK_DAYS", "30"))
YCLIENTS_SYNC_INTERVAL_SEC = int(os.getenv("YCLIENTS_SYNC_INTERVAL_SEC", "90"))
# Сколько минут считаем запись «ботовской» после клика на кнопку записи
BOT_BOOKING_WINDOW_MIN = int(os.getenv("BOT_BOOKING_WINDOW_MIN", "30"))


def _normalize_phone(phone: str | None) -> str | None:
    """
    Приводим номер к формату 7XXXXXXXXXX (только цифры).
    Поддерживаем входные варианты с +7, 7, 8, пробелами/скобками.
    """
    if not phone:
        return None
    digits = re.sub(r"\D", "", str(phone))
    if not digits:
        return None
    if len(digits) == 11 and digits.startswith("8"):
        digits = "7" + digits[1:]
    if len(digits) == 10:
        digits = "7" + digits
    return digits if len(digits) == 11 else None


def _phone_variants(normalized: str | None) -> list[str]:
    if not normalized:
        return []
    variants = {normalized}
    if normalized.startswith("7") and len(normalized) == 11:
        core = normalized[1:]
        variants.update({"+7" + core, "8" + core, core})
    return list(variants)


def _parse_record_datetime(record: dict) -> datetime:
    """
    Пытаемся достать дату записи из ответа YClients.
    fallback: now (UTC), чтобы не упасть.
    """
    dt_fields = ["create_date", "date", "datetime", "start_time"]
    for key in dt_fields:
        val = record.get(key)
        if not val:
            continue
        try:
            # приводим Z к +00:00 для fromisoformat
            if isinstance(val, str):
                val_iso = val.replace("Z", "+00:00")
                return datetime.fromisoformat(val_iso)
            # если это timestamp (int/float)
            if isinstance(val, (int, float)):
                return datetime.utcfromtimestamp(val)
        except Exception:
            continue
    return datetime.utcnow()


def _parse_dt_fields(record: dict, keys: list[str]) -> datetime | None:
    """
    Универсальный парсер по списку полей даты.
    Используем, например, для create_date.
    """
    for key in keys:
        val = record.get(key)
        if not val:
            continue
        try:
            if isinstance(val, str):
                return datetime.fromisoformat(str(val).replace("Z", "+00:00"))
            if isinstance(val, (int, float)):
                return datetime.utcfromtimestamp(val)
        except Exception:
            continue
    return None


def _to_naive_utc(dt: datetime | None) -> datetime | None:
    if dt is None:
        return None
    if dt.tzinfo:
        return dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt


def _extract_client_id(payload: dict) -> int | None:
    # Популярные варианты: client_id или client.id
    if "client_id" in payload:
        try:
            return int(payload.get("client_id"))
        except Exception:
            return None
    client = payload.get("client") or {}
    try:
        cid = client.get("id")
        return int(cid) if cid is not None else None
    except Exception:
        return None


def _extract_record_id(payload: dict) -> int | None:
    for key in ("id", "record_id", "booking_id"):
        if key in payload:
            try:
                return int(payload.get(key))
            except Exception:
                continue
    return None


def _map_record_status(record: dict) -> BookingEventType:
    """
    Минимальная эвристика:
    - если статус содержит cancel/delete -> CANCELLED
    - если visit/completed/done/finished или оплата -> COMPLETED
    - иначе CREATED
    """
    status_raw = (
        record.get("attendance")
        or record.get("visit_attendance")
        or record.get("status")
        or ""
    )
    # Числовые коды посещаемости YClients: 2 — посетил/завершён; 4 — отменён/не пришёл
    num_status = None
    try:
        num_status = int(status_raw)
    except Exception:
        num_status = None

    status = str(status_raw).lower()
    payment_status_raw = record.get("payment_status", "")
    is_paid = bool(
        record.get("is_payed")
        or record.get("is_paid")
        or record.get("paid_full")
        or str(payment_status_raw).lower() == "paid"
        or (
            isinstance(payment_status_raw, (int, float))
            and payment_status_raw > 0
        )
    )

    if record.get("deleted"):
        return BookingEventType.CANCELLED

    if num_status is not None:
        if num_status == 2:
            return BookingEventType.COMPLETED
        if num_status in (4, 5):
            return BookingEventType.CANCELLED

    if "cancel" in status or "delete" in status:
        return BookingEventType.CANCELLED
    if "visit" in status or "completed" in status or "done" in status or "finish" in status or is_paid:
        return BookingEventType.COMPLETED
    return BookingEventType.CREATED


async def _log_event_if_needed(
    session: AsyncSession,
    user: User,
    record_id: int,
    event_type: BookingEventType,
) -> bool:
    # Если приходит отмена — удаляем возможные COMPLETED для этого record_id,
    # чтобы визит не числился одновременно выполненным и отменённым.
    if event_type == BookingEventType.CANCELLED:
        await session.execute(
            delete(BookingEvent).where(
                BookingEvent.event_type == BookingEventType.COMPLETED,
                cast(BookingEvent.meta["record_id"].astext, String) == str(record_id),
            )
        )
        await session.commit()

    exists = await session.scalar(
        select(func.count(BookingEvent.id)).where(
            BookingEvent.event_type == event_type,
            cast(BookingEvent.meta["record_id"].astext, String) == str(record_id),
        )
    )
    if exists:
        return False

    await log_booking_event(
        session=session,
        user=user,
        event_type=event_type,
        yclients_record_id=record_id,
        meta={"source": "yclients_polling"},
    )
    return True


async def sync_yclients_records_once():
    """
    Раз за вызов подтягиваем записи за последние YCLIENTS_SYNC_LOOKBACK_DAYS,
    логируем CREATED/COMPLETED/CANCELLED и возвращаем статистику.

    Окно симметричное: прошлое и будущее по YCLIENTS_SYNC_LOOKBACK_DAYS,
    чтобы ловить и прошедшие, и будущие записи (например, запись на 20 декабря при сегодняшней дате 9 декабря).
    """
    client = YClientsClient()
    today = date.today()
    start = today - timedelta(days=YCLIENTS_SYNC_LOOKBACK_DAYS)
    end = today + timedelta(days=YCLIENTS_SYNC_LOOKBACK_DAYS)
    records = await client.get_all_records(start_date=start, end_date=end)
    if not records:
        return {"success": True, "processed": 0, "created": 0, "completed": 0, "cancelled": 0}

    stats = {"success": True, "processed": 0, "created": 0, "completed": 0, "cancelled": 0}

    async with AsyncSessionLocal() as session:
        for record in records:
            record_id = _extract_record_id(record)
            client_id = _extract_client_id(record)  # fallback, если найдём по id
            client_block = record.get("client") or {}
            phone_raw = (
                client_block.get("phone")
                or record.get("phone")
                or record.get("client_phone")
                or ""
            )
            normalized_phone = _normalize_phone(phone_raw)

            if not record_id:
                continue

            user = None
            if normalized_phone:
                variants = _phone_variants(normalized_phone)
                user = await session.scalar(select(User).where(User.phone.in_(variants)))

            if not user and client_id:
                user = await get_user_by_yclients_id(session, client_id)

            if not user:
                continue

            # Привязываем только к бот-кликам: ищем последний клик и сравниваем окно
            record_dt = _to_naive_utc(_parse_record_datetime(record))
            created_dt = _to_naive_utc(
                _parse_dt_fields(record, ["create_date", "created_at", "datetime", "date"])
            )
            reference_dt = created_dt or record_dt
            last_click = await session.scalar(
                select(BookingEvent.created_at)
                .where(
                    BookingEvent.user_id == user.id,
                    BookingEvent.event_type == BookingEventType.CLICK_BOOKING,
                )
                .order_by(BookingEvent.created_at.desc())
                .limit(1)
            )

            if not last_click:
                continue
            last_click = _to_naive_utc(last_click)

            if reference_dt is None or last_click is None:
                continue

            delta = reference_dt - last_click
            if delta.total_seconds() < 0:
                delta = -delta
            if delta > timedelta(minutes=BOT_BOOKING_WINDOW_MIN):
                continue

            status_event = _map_record_status(record)

            # Если уже есть CANCELLED для этого record_id — не возвращаем COMPLETED обратно
            if status_event == BookingEventType.COMPLETED:
                cancelled_exists = await session.scalar(
                    select(func.count(BookingEvent.id)).where(
                        BookingEvent.event_type == BookingEventType.CANCELLED,
                        cast(BookingEvent.meta["record_id"].astext, String) == str(record_id),
                    )
                )
                if cancelled_exists:
                    continue

            events_to_log = {BookingEventType.CREATED}
            if status_event == BookingEventType.CANCELLED:
                events_to_log.add(BookingEventType.CANCELLED)
            elif status_event == BookingEventType.COMPLETED:
                events_to_log.add(BookingEventType.COMPLETED)

            for ev in events_to_log:
                added = await _log_event_if_needed(session, user, record_id, ev)
                if added:
                    stats[ev.value.lower()] = stats.get(ev.value.lower(), 0) + 1
            stats["processed"] += 1

    return stats


# static + templates
app.mount("/static", StaticFiles(directory="admin/static"), name="static")
templates = Jinja2Templates(directory="admin/templates")

# =========================
#   ПРОСТАЯ COOKIE-АВТОРИЗАЦИЯ
# =========================

SESSIONS: dict[str, str] = {}

ADMIN_USERNAME = os.getenv("ADMIN_USERNAME", "admin")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "admin")


async def get_db() -> AsyncSession:
    async with AsyncSessionLocal() as session:
        yield session


async def get_current_admin(request: Request) -> str:
    """
    Проверяем cookie admin_session.
    Если токен не найден или невалиден — уводим на /login.
    """
    token = request.cookies.get("admin_session")
    if not token or token not in SESSIONS:
        raise HTTPException(
            status_code=302,
            detail="Redirect",
            headers={"Location": "/login"},
        )
    return SESSIONS[token]



@app.post("/sync-bookings", name="sync_bookings")
async def sync_bookings(
    request: Request,
    db: AsyncSession = Depends(get_db),
    current_admin: str = Depends(get_current_admin),
):
    """Ручной/авто запуск синка записей YClients без вебхука."""
    stats = await sync_yclients_records_once()
    return JSONResponse(stats)

# =========================
#   ВСПОМОГАТЕЛЬНЫЕ ШТУКИ ДЛЯ TELEGRAM
# =========================

def _get_review_chat_id(review: ReviewBonusRequest, user: User | None = None):
    """
    Аккуратно достаём chat_id:
    1) если есть user и у него есть telegram_id — берём его,
    2) иначе берём review.telegram_user_id,
    3) иначе None.
    """
    if user is not None and getattr(user, "telegram_id", None):
        return user.telegram_id
    if getattr(review, "telegram_user_id", None):
        return review.telegram_user_id
    return None


async def _send_telegram_message(chat_id, text: str):
    """
    Безопасная отправка сообщения:
    - если нет BOT_TOKEN или chat_id — выходим,
    - любые ошибки логируем, но не роняем админку.
    """
    token = os.getenv("BOT_TOKEN")
    if not token or not chat_id:
        return

    bot = Bot(token)
    try:
        await bot.send_message(chat_id=chat_id, text=text)
    except Exception:
        logger.exception("Failed to send Telegram message", extra={"chat_id": chat_id})
    finally:
        await bot.session.close()


def _user_display_name(user: User) -> str:
    """
    Красивое имя для логов: сначала full_name, потом @username, потом TG:ID, потом просто ID.
    """
    if getattr(user, "full_name", None):
        return user.full_name
    if getattr(user, "username", None):
        return f"@{user.username}"
    if getattr(user, "telegram_id", None):
        return f"TG:{user.telegram_id}"
    return f"ID {getattr(user, 'id', '?')}"


# =========================
#   ВСПОМОГАТЕЛЬНЫЕ ШТУКИ ДЛЯ AUDIT LOG (читаемые подписи)
# =========================

def _human_action_label(action: str) -> str:
    mapping = {
        "BONUS_MANUAL_ACCRUAL": "Ручное начисление бонусов",
        "BONUS_MANUAL_WRITE_OFF": "Ручное списание бонусов",
        "BONUS_MANUAL_BULK_ACCRUAL": "Массовое начисление бонусов",
        "REVIEW_CONFIRM": "Подтверждение отзыва",
        "REVIEW_REJECT": "Отказ по отзыву",
        "BONUS_CONFIG_UPDATE": "Изменение настроек бонусов",
        "BROADCAST_SEND": "Рассылка уведомлений",
        "BOOKING_CANCEL": "Отмена записи",
        "BOT_TEXT_UPDATE": "Изменение текста бота",
        "PROMOCODE_CREATE": "Создание промокода",
        "PROMOCODE_DELETE": "Удаление промокода",
        "PROMOCODE_TOGGLE": "Изменение статуса промокода",
        "SETTINGS_UPDATE": "Изменение настроек",
    }
    action = action or ""
    return mapping.get(action, action or "Неизвестное действие")


def _human_entity_label(entity_type: str | None, entity_id: int | None) -> str:
    if not entity_type:
        return "—"

    mapping = {
        "user_bonus": "Бонусы клиента",
        "user_bonus_bulk": "Массовое начисление бонусов",
        "review_request": "Заявка на бонус за отзыв",
        "bonus_config": "Настройки бонусной программы",
        "broadcast": "Рассылка уведомлений",
        "yclients_record": "Запись YClients",
        "bot_text": "Текст бота",
        "promocode": "Промокод",
    }
    base = mapping.get(entity_type, entity_type.replace("_", " ").title())
    if entity_id is not None and entity_id != 0:
        return f"{base} · ID {entity_id}"
    return base


def _human_payload_details(
    action: str,
    payload_str: str | None,
    entity_type: str | None,
    entity_id: int | None,
) -> str:
    data: dict = {}
    if payload_str:
        try:
            data = json.loads(payload_str)
        except Exception:
            # Если JSON битый — покажем как есть
            return f"Дополнительные данные: {payload_str}"

    action = action or ""

    # Ручные операции по бонусам (одному клиенту)
    if action in ("BONUS_MANUAL_ACCRUAL", "BONUS_MANUAL_WRITE_OFF"):
        user_id = data.get("user_id") or entity_id
        amount = data.get("amount")
        delta = data.get("delta")
        comment = data.get("comment")
        balance_after = data.get("balance_after")
        user_name = (
            data.get("user_name")
            or data.get("user_full_name")
            or data.get("user_username")
        )

        if delta is not None and delta < 0:
            op = "Списание бонусов"
        else:
            op = "Начисление бонусов"

        parts: list[str] = []
        if user_name:
            parts.append(f"{op} клиенту {user_name}.")
        elif user_id is not None:
            parts.append(f"{op} клиенту (ID {user_id}).")
        else:
            parts.append(f"{op} клиенту.")

        if amount is not None:
            parts.append(f" Сумма операции: {amount}₽.")
        if balance_after is not None:
            parts.append(f" Баланс после операции: {balance_after}₽.")
        if comment:
            parts.append(f" Комментарий администратора: «{comment}».")
        return "".join(parts)

    # Массовое начисление
    if action == "BONUS_MANUAL_BULK_ACCRUAL":
        amount = data.get("amount")
        comment = data.get("comment")
        processed_ids = data.get("processed_ids") or []
        not_found_ids = data.get("not_found_ids") or []

        parts: list[str] = []
        parts.append(f"Массовое начисление бонусов {len(processed_ids)} клиентам.")
        if amount is not None:
            parts.append(f" Каждому начислено по {amount}₽.")
        if comment:
            parts.append(f" Комментарий: «{comment}».")
        if not_found_ids:
            parts.append(
                " Некоторые ID клиентов не найдены: "
                + ", ".join(str(x) for x in not_found_ids)
                + "."
            )
        return "".join(parts)

    # Подтверждение отзыва
    if action == "REVIEW_CONFIRM":
        user_id = data.get("user_id") or entity_id
        amount = data.get("amount")
        comment = data.get("comment")
        user_name = data.get("user_name")

        parts: list[str] = []
        if user_name:
            parts.append(f"Бонус за отзыв клиенту {user_name}.")
        elif user_id:
            parts.append(f"Бонус за отзыв клиенту (ID {user_id}).")
        else:
            parts.append("Бонус за отзыв клиенту.")
        if amount is not None:
            parts.append(f" Начислено: {amount}₽.")
        if comment:
            parts.append(f" Комментарий: «{comment}».")
        return "".join(parts)

    # Отказ по отзыву
    if action == "REVIEW_REJECT":
        comment = data.get("comment")
        if comment:
            return f"Заявка на бонус за отзыв отклонена. Комментарий: «{comment}»."
        return "Заявка на бонус за отзыв отклонена без комментария."

    # Изменение настроек бонусов
    if action == "BONUS_CONFIG_UPDATE":
        wa = data.get("welcome_amount")
        ca = data.get("channel_amount")
        ra = data.get("review_amount")
        rfa = data.get("referral_amount")

        parts: list[str] = []
        parts.append("Обновлены настройки бонусной программы:")
        if wa is not None:
            parts.append(f" welcome bonus = {wa}₽;")
        if ca is not None:
            parts.append(f" за подписку на канал = {ca}₽;")
        if ra is not None:
            parts.append(f" за отзыв = {ra}₽;")
        if rfa is not None:
            parts.append(f" реферальный бонус = {rfa}₽.")
        return "".join(parts)

    # Рассылка уведомлений
    if action == "BROADCAST_SEND":
        message_text = data.get("message_text", "")
        sent_ids = data.get("sent_ids") or []
        failed_ids = data.get("failed_ids") or []
        not_found_ids = data.get("not_found_ids") or []
        total_selected = data.get("total_selected", 0)

        parts: list[str] = []
        parts.append(f"Рассылка уведомлений {len(sent_ids)} пользователям.")
        if failed_ids:
            parts.append(f" Ошибки отправки: {len(failed_ids)}.")
        if not_found_ids:
            parts.append(f" Не найдены: {len(not_found_ids)}.")
        if message_text:
            preview = message_text[:100] + "..." if len(message_text) > 100 else message_text
            parts.append(f" Текст: «{preview}»")
        return "".join(parts)

    # Отмена записи YCLIENTS
    if action == "BOOKING_CANCEL":
        record_id = data.get("record_id") or entity_id
        success = data.get("success")
        client_name = data.get("client_name")
        client_phone = data.get("client_phone")
        staff_name = data.get("staff_name")
        dt = data.get("datetime")

        parts: list[str] = []
        if success:
            parts.append(f"Запись #{record_id} отменена.")
        else:
            parts.append(f"Ошибка отмены записи #{record_id}.")
        if client_name:
            parts.append(f" Клиент: {client_name}.")
        if client_phone:
            parts.append(f" Телефон: {client_phone}.")
        if staff_name:
            parts.append(f" Мастер: {staff_name}.")
        if dt:
            parts.append(f" Дата: {dt[:16].replace('T', ' ')}.")
        return "".join(parts)

    # Изменение текста бота
    if action == "BOT_TEXT_UPDATE":
        text_key = data.get("text_key")
        old_value = data.get("old_value", "")
        new_value = data.get("new_value", "")
        
        parts: list[str] = []
        if text_key:
            parts.append(f"Обновлён текст: {text_key}.")
        if old_value and new_value:
            old_preview = old_value[:50] + "..." if len(old_value) > 50 else old_value
            new_preview = new_value[:50] + "..." if len(new_value) > 50 else new_value
            parts.append(f" Было: «{old_preview}». Стало: «{new_preview}».")
        return "".join(parts)

    # Создание промокода
    if action == "PROMOCODE_CREATE":
        code = data.get("code")
        bonus_amount = data.get("bonus_amount")
        max_uses = data.get("max_uses")
        description = data.get("description")
        
        parts: list[str] = []
        if code:
            parts.append(f"Создан промокод: {code}.")
        if bonus_amount is not None:
            parts.append(f" Бонус: {bonus_amount}₽.")
        if max_uses is not None:
            parts.append(f" Максимум использований: {max_uses}.")
        if description:
            parts.append(f" Описание: «{description}».")
        return "".join(parts)

    # Удаление промокода
    if action == "PROMOCODE_DELETE":
        code = data.get("code")
        if code:
            return f"Удалён промокод: {code}."
        return "Удалён промокод."

    # Изменение статуса промокода
    if action == "PROMOCODE_TOGGLE":
        code = data.get("code")
        is_active = data.get("is_active")
        
        parts: list[str] = []
        if code:
            parts.append(f"Промокод {code}: ")
        else:
            parts.append("Промокод: ")
        if is_active:
            parts.append("активирован.")
        else:
            parts.append("деактивирован.")
        return "".join(parts)

    # Общий случай: форматируем JSON красиво
    if data:
        parts: list[str] = []
        for key, value in data.items():
            # Пропускаем служебные поля
            if key in ("user_id", "entity_id", "entity_type", "quick"):
                continue
            
            # Форматируем ключи в читаемый вид
            human_key = key.replace("_", " ").title()
            
            if isinstance(value, (int, float)):
                if "amount" in key.lower() or "bonus" in key.lower() or "balance" in key.lower():
                    parts.append(f"{human_key}: {value}₽")
                elif "count" in key.lower() or "uses" in key.lower() or "id" in key.lower():
                    parts.append(f"{human_key}: {value}")
                else:
                    parts.append(f"{human_key}: {value}")
            elif isinstance(value, bool):
                parts.append(f"{human_key}: {'Да' if value else 'Нет'}")
            elif isinstance(value, str):
                if len(value) > 100:
                    parts.append(f"{human_key}: «{value[:100]}...»")
                else:
                    parts.append(f"{human_key}: «{value}»")
            elif isinstance(value, (list, tuple)):
                if len(value) <= 5:
                    parts.append(f"{human_key}: {', '.join(str(v) for v in value)}")
                else:
                    parts.append(f"{human_key}: {len(value)} элементов")
            else:
                parts.append(f"{human_key}: {value}")
        
        if parts:
            return ". ".join(parts) + "."
    
    # Если ничего не подошло
    if payload_str:
        return f"Дополнительные данные: {payload_str}"
    
    return "—"

    # Фоллбек — если вдруг появятся новые типы действий
    if data:
        compact = json.dumps(data, ensure_ascii=False)
        return f"Дополнительные данные: {compact}"
    return "Нет дополнительных данных."


# =========================
#   AUTH ROUTES
# =========================

@app.get("/login", response_class=HTMLResponse, name="login_page")
async def login_page(request: Request):
    return templates.TemplateResponse(
        "login.html",
        {"request": request, "error": None},
    )


@app.post("/login", response_class=HTMLResponse)
async def login_submit(
    request: Request,
    username: str = Form(...),
    password: str = Form(...),
):
    if username != ADMIN_USERNAME or password != ADMIN_PASSWORD:
        return templates.TemplateResponse(
            "login.html",
            {
                "request": request,
                "error": "Неверный логин или пароль",
            },
            status_code=401,
        )

    token = secrets.token_hex(32)
    SESSIONS[token] = username

    resp = RedirectResponse(url="/", status_code=302)
    resp.set_cookie(
        "admin_session",
        token,
        httponly=True,
        secure=False,  # под HTTPS можно выставить True
        samesite="lax",
        max_age=60 * 60 * 8,  # 8 часов
    )
    return resp


@app.get("/logout")
async def logout(request: Request):
    token = request.cookies.get("admin_session")
    if token and token in SESSIONS:
        del SESSIONS[token]
    resp = RedirectResponse(url="/login", status_code=302)
    resp.delete_cookie("admin_session")
    return resp


# =========================
#   DASHBOARD & USERS
# =========================

@app.get("/", response_class=HTMLResponse, name="dashboard")
async def dashboard(
    request: Request,
    db: AsyncSession = Depends(get_db),
    current_admin: str = Depends(get_current_admin),
    period: int = 30,
):
    # Валидируем период
    if period not in (7, 14, 30, 90):
        period = 30

    # Живой синк записей перед расчётом метрик
    try:
        await sync_yclients_records_once()
    except Exception:
        logger.exception("sync_yclients_records_once failed on dashboard")
    
    now = datetime.utcnow()
    week_ago = now - timedelta(days=7)
    month_ago = now - timedelta(days=30)
    period_ago = now - timedelta(days=period)

    total_users = await db.scalar(select(func.count(User.id)))
    new_last_week = await db.scalar(
        select(func.count(User.id)).where(User.created_at >= week_ago)
    )
    new_last_month = await db.scalar(
        select(func.count(User.id)).where(User.created_at >= month_ago)
    )
    total_bonus = await db.scalar(
        select(func.coalesce(func.sum(UserBonus.balance), 0))
    )

    # === ЗАПИСИ: создано/выполнено/отменено за выбранный период ===
    bookings_created = await db.scalar(
        select(func.count(BookingEvent.id)).where(
            BookingEvent.created_at >= period_ago,
            BookingEvent.event_type == BookingEventType.CREATED,
        )
    ) or 0

    bookings_completed = await db.scalar(
        select(func.count(BookingEvent.id)).where(
            BookingEvent.created_at >= period_ago,
            BookingEvent.event_type == BookingEventType.COMPLETED,
        )
    ) or 0

    bookings_cancelled = await db.scalar(
        select(func.count(BookingEvent.id)).where(
            BookingEvent.created_at >= period_ago,
            BookingEvent.event_type == BookingEventType.CANCELLED,
        )
    ) or 0

    # === БОНУСЫ: начислено/списано за выбранный период ===
    bonus_accrued = await db.scalar(
        select(func.coalesce(func.sum(BonusTransaction.amount), 0)).where(
            BonusTransaction.created_at >= period_ago,
            BonusTransaction.amount > 0,
        )
    ) or 0

    bonus_spent_raw = await db.scalar(
        select(func.coalesce(func.sum(BonusTransaction.amount), 0)).where(
            BonusTransaction.created_at >= period_ago,
            BonusTransaction.amount < 0,
        )
    ) or 0
    bonus_spent = abs(bonus_spent_raw)

    # === ВЫПОЛНЕННЫЕ ЗАДАНИЯ за выбранный период (по транзакциям) ===
    tasks_welcome = await db.scalar(
        select(func.count(BonusTransaction.id)).where(
            BonusTransaction.created_at >= period_ago,
            BonusTransaction.source == BonusTransactionSource.WELCOME,
        )
    ) or 0

    tasks_channel = await db.scalar(
        select(func.count(BonusTransaction.id)).where(
            BonusTransaction.created_at >= period_ago,
            BonusTransaction.source == BonusTransactionSource.SUBSCRIPTION,
        )
    ) or 0

    tasks_review = await db.scalar(
        select(func.count(BonusTransaction.id)).where(
            BonusTransaction.created_at >= period_ago,
            BonusTransaction.source == BonusTransactionSource.REVIEW,
        )
    ) or 0

    tasks_referral = await db.scalar(
        select(func.count(BonusTransaction.id)).where(
            BonusTransaction.created_at >= period_ago,
            BonusTransaction.source == BonusTransactionSource.REFERRAL,
        )
    ) or 0

    # топ рефоводов по заработанным реф-бонусам
    top_referrers_q = (
        select(User, UserBonus.referral_earned)
        .join(UserBonus, UserBonus.user_id == User.id)
        .where(UserBonus.referral_earned > 0)
        .order_by(UserBonus.referral_earned.desc())
        .limit(5)
    )
    res = await db.execute(top_referrers_q)
    top_referrers = res.all()

    # активность по дням (регистрация юзеров за 14 дней)
    days_back = 14
    activity_q = (
        select(
            func.date_trunc("day", User.created_at).label("day"),
            func.count(User.id),
        )
        .where(User.created_at >= now - timedelta(days=days_back))
        .group_by("day")
        .order_by("day")
    )
    act_res = await db.execute(activity_q)
    act_rows = act_res.all()
    activity_labels = [row[0].strftime("%d.%m") for row in act_rows]
    activity_values = [row[1] for row in act_rows]

    return templates.TemplateResponse(
        "dashboard.html",
        {
            "request": request,
            "total_users": total_users or 0,
            "new_last_week": new_last_week or 0,
            "new_last_month": new_last_month or 0,
            "total_bonus": total_bonus or 0,
            "top_referrers": top_referrers,
            "activity_labels": activity_labels,
            "activity_values": activity_values,
            # Новые данные
            "bookings_created": bookings_created,
            "bookings_completed": bookings_completed,
            "bookings_cancelled": bookings_cancelled,
            "bonus_accrued": int(bonus_accrued),
            "bonus_spent": int(bonus_spent),
            "tasks_welcome": tasks_welcome,
            "tasks_channel": tasks_channel,
            "tasks_review": tasks_review,
            "tasks_referral": tasks_referral,
            "period": period,
        },
    )


@app.get("/analytics", response_class=HTMLResponse, name="analytics")
async def analytics_page(
    request: Request,
    db: AsyncSession = Depends(get_db),
    current_admin: str = Depends(get_current_admin),
):
    """
    Аналитика по бонусной программе и вовлечённости клиентов.
    """
    now = datetime.utcnow()
    since = now - timedelta(days=30)

    # ==== KPI по бонусам (в целом) ====

    # всего начислено (по всем времени)
    total_accrual = await db.scalar(
        select(func.coalesce(func.sum(BonusTransaction.amount), 0)).where(
            BonusTransaction.amount > 0
        )
    )

    # всего списано (берём модуль отрицательных сумм)
    total_writeoff_raw = await db.scalar(
        select(func.coalesce(func.sum(BonusTransaction.amount), 0)).where(
            BonusTransaction.amount < 0
        )
    )
    total_writeoff = abs(total_writeoff_raw or 0)

    # обязательство по бонусам — сумма балансов
    obligations = await db.scalar(
        select(func.coalesce(func.sum(UserBonus.balance), 0))
    )

    # ==== Динамика начислений/списаний за последние 30 дней ====

    flow_q = (
        select(
            func.date_trunc("day", BonusTransaction.created_at).label("day"),
            func.coalesce(
                func.sum(
                    case(
                        (BonusTransaction.amount > 0, BonusTransaction.amount),
                        else_=0,
                    )
                ),
                0,
            ).label("accrual"),
            func.coalesce(
                func.sum(
                    case(
                        (BonusTransaction.amount < 0, -BonusTransaction.amount),
                        else_=0,
                    )
                ),
                0,
            ).label("writeoff"),
        )
        .where(BonusTransaction.created_at >= since)
        .group_by("day")
        .order_by("day")
    )

    flow_res = await db.execute(flow_q)
    flow_rows = flow_res.all()

    flow_labels: list[str] = []
    flow_accrual: list[int] = []
    flow_writeoff: list[int] = []

    for row in flow_rows:
        day, acc, wt = row
        flow_labels.append(day.strftime("%d.%m"))
        flow_accrual.append(int(acc or 0))
        flow_writeoff.append(int(wt or 0))

    # ==== Маркетинговые метрики (как и раньше) ====

    total_users = await db.scalar(select(func.count(User.id))) or 0

    channel_users = await db.scalar(
        select(func.count(UserBonus.user_id)).where(UserBonus.channel_given.is_(True))
    ) or 0

    channel_share = float(channel_users) / total_users * 100 if total_users > 0 else 0.0

    confirmed_reviews = await db.scalar(
        select(func.count(ReviewBonusRequest.id)).where(
            ReviewBonusRequest.status == ReviewRequestStatus.CONFIRMED
        )
    ) or 0

    # среднее время модерации (подтверждённые заявки)
    avg_moderation_seconds = await db.scalar(
        select(
            func.avg(
                func.extract(
                    "epoch",
                    ReviewBonusRequest.decided_at - ReviewBonusRequest.created_at,
                )
            )
        ).where(
            ReviewBonusRequest.status == ReviewRequestStatus.CONFIRMED,
            ReviewBonusRequest.decided_at.is_not(None),
        )
    )

    if avg_moderation_seconds is None:
        avg_moderation_minutes: int | None = None
    else:
        avg_moderation_minutes = int(avg_moderation_seconds // 60)

    # ============================================================
    #   НОВОЕ: ONBOARDING-ВОРОНКА
    # ============================================================

    def _rate(part: int, whole: int) -> float:
        if not whole:
            return 0.0
        return round(float(part) / float(whole) * 100.0, 1)

    # пользователи, оставившие телефон
    onboarding_phone = await db.scalar(
        select(func.count(User.id)).where(User.phone.is_not(None))
    ) or 0

    # подтвердили политику (условно «доверие / завершённый онбординг»)
    onboarding_privacy = await db.scalar(
        select(func.count(User.id)).where(User.agreed_privacy.is_(True))
    ) or 0

    # пользователи, у которых была хотя бы одна бонусная транзакция (amount != 0)
    onboarding_bonus_users = await db.scalar(
        select(func.count(func.distinct(BonusTransaction.user_id))).where(
            BonusTransaction.amount != 0
        )
    ) or 0

    # пользователи, у которых были события по записям
    onboarding_booking_users = await db.scalar(
        select(func.count(func.distinct(BookingEvent.user_id))).where(
            BookingEvent.user_id.is_not(None)
        )
    ) or 0

    onboarding_steps = [
        {
            "code": "start",
            "label": "Старт бота",
            "value": int(total_users),
            "pct": _rate(int(total_users), int(total_users)),
        },
        {
            "code": "phone",
            "label": "Оставили телефон",
            "value": int(onboarding_phone),
            "pct": _rate(int(onboarding_phone), int(total_users)),
        },
        {
            "code": "privacy",
            "label": "Приняли политику и условия",
            "value": int(onboarding_privacy),
            "pct": _rate(int(onboarding_privacy), int(total_users)),
        },
        {
            "code": "bonus",
            "label": "Получали бонусы / участвовали в акциях",
            "value": int(onboarding_bonus_users),
            "pct": _rate(int(onboarding_bonus_users), int(total_users)),
        },
        {
            "code": "booking",
            "label": "Делали записи через бота",
            "value": int(onboarding_booking_users),
            "pct": _rate(int(onboarding_booking_users), int(total_users)),
        },
    ]

    # ============================================================
    #   НОВОЕ: ВОРОНКА БОНУСНЫХ ЗАДАЧ
    # ============================================================

    welcome_users = await db.scalar(
        select(func.count(UserBonus.id)).where(UserBonus.welcome_given.is_(True))
    ) or 0

    channel_bonus_users = await db.scalar(
        select(func.count(UserBonus.id)).where(UserBonus.channel_given.is_(True))
    ) or 0

    review_bonus_users = await db.scalar(
        select(func.count(UserBonus.id)).where(
            (UserBonus.review_yandex_given.is_(True)) |
            (UserBonus.review_2gis_given.is_(True))
        )
    ) or 0

    referral_bonus_users = await db.scalar(
        select(func.count(UserBonus.id)).where(
            (UserBonus.referral_earned > 0) |
            (UserBonus.referral_visit_reward_given.is_(True))
        )
    ) or 0

    bonus_tasks = [
        {
            "code": "welcome",
            "name": "Приветственный бонус",
            "completed_users": int(welcome_users),
            "coverage_pct": _rate(int(welcome_users), int(total_users)),
        },
        {
            "code": "channel",
            "name": "Подписка на Telegram-канал",
            "completed_users": int(channel_bonus_users),
            "coverage_pct": _rate(int(channel_bonus_users), int(total_users)),
        },
        {
            "code": "review",
            "name": "Бонусы за отзывы (Яндекс / 2ГИС)",
            "completed_users": int(review_bonus_users),
            "coverage_pct": _rate(int(review_bonus_users), int(total_users)),
        },
        {
            "code": "referral",
            "name": "Реферальная программа",
            "completed_users": int(referral_bonus_users),
            "coverage_pct": _rate(int(referral_bonus_users), int(total_users)),
        },
    ]

    # ============================================================
    #   ВОРОНКА ЗАПИСЕЙ (по типам событий)
    # ============================================================

    # Названия типов событий для отображения
    event_type_labels = {
        BookingEventType.CLICK_BOOKING: "Нажали «Записаться»",
        BookingEventType.CREATED: "Записей создано",
        BookingEventType.COMPLETED: "Визитов состоялось",
        BookingEventType.CANCELLED: "Записей отменено",
    }

    # Получаем статистику по каждому типу события
    booking_funnel = []
    for event_type in [BookingEventType.CLICK_BOOKING, BookingEventType.CREATED, BookingEventType.COMPLETED, BookingEventType.CANCELLED]:
        count = await db.scalar(
            select(func.count(BookingEvent.id)).where(BookingEvent.event_type == event_type)
        ) or 0
        booking_funnel.append({
            "label": event_type_labels.get(event_type, event_type.value),
            "count": count,
            "event_type": event_type.value,
        })

    # ============================================================
    #   ЭФФЕКТИВНОСТЬ ЗАДАНИЙ
    # ============================================================

    # Доля пользователей с welcome, которые сделали визит (COMPLETED booking)
    welcome_with_visit = await db.scalar(
        select(func.count(func.distinct(UserBonus.user_id))).where(
            UserBonus.welcome_given.is_(True),
            UserBonus.user_id.in_(
                select(BookingEvent.user_id).where(
                    BookingEvent.event_type == BookingEventType.COMPLETED,
                    BookingEvent.user_id.is_not(None),
                )
            )
        )
    ) or 0

    welcome_effectiveness = _rate(welcome_with_visit, welcome_users) if welcome_users > 0 else 0.0

    # Реферальная статистика
    # Количество пользователей, у которых есть referred_by_code (приглашённые)
    invited_users = await db.scalar(
        select(func.count(UserBonus.id)).where(
            UserBonus.referred_by_code.is_not(None)
        )
    ) or 0

    # Приглашённые, которые сделали визит
    invited_with_visit = await db.scalar(
        select(func.count(UserBonus.id)).where(
            UserBonus.referred_by_code.is_not(None),
            UserBonus.referral_visit_reward_given.is_(True),
        )
    ) or 0

    referral_conversion = _rate(invited_with_visit, invited_users) if invited_users > 0 else 0.0

    # Среднее количество приглашённых на одного реферера
    # Считаем: сколько разных referred_by_code и сколько инвайтеров
    inviters_count = await db.scalar(
        select(func.count(func.distinct(UserBonus.referral_code))).where(
            UserBonus.referral_code.in_(
                select(UserBonus.referred_by_code).where(
                    UserBonus.referred_by_code.is_not(None)
                )
            )
        )
    ) or 0

    avg_invites_per_referrer = round(invited_users / inviters_count, 1) if inviters_count > 0 else 0.0

    effectiveness_stats = {
        "welcome_users": welcome_users,
        "welcome_with_visit": welcome_with_visit,
        "welcome_effectiveness": welcome_effectiveness,
        "invited_users": invited_users,
        "invited_with_visit": invited_with_visit,
        "referral_conversion": referral_conversion,
        "inviters_count": inviters_count,
        "avg_invites_per_referrer": avg_invites_per_referrer,
    }

    return templates.TemplateResponse(
        "analytics.html",
        {
            "request": request,
            # KPI по бонусам
            "total_accrual": int(total_accrual or 0),
            "total_writeoff": int(total_writeoff or 0),
            "obligations": int(obligations or 0),
            # график начислено/списано
            "flow_labels": flow_labels,
            "flow_accrual": flow_accrual,
            "flow_writeoff": flow_writeoff,
            # маркетинговые метрики
            "total_users": total_users,
            "channel_users": channel_users,
            "channel_share": round(channel_share, 1),
            "confirmed_reviews": confirmed_reviews,
            "avg_moderation_minutes": avg_moderation_minutes,
            # onboarding-воронка
            "onboarding_steps": onboarding_steps,
            # воронка бонусных задач
            "bonus_tasks": bonus_tasks,
            # воронка записей
            "booking_funnel": booking_funnel,
            # эффективность заданий
            "effectiveness_stats": effectiveness_stats,
        },
    )

@app.get("/users", response_class=HTMLResponse, name="users_list")
async def users_list(
    request: Request,
    db: AsyncSession = Depends(get_db),
    current_admin: str = Depends(get_current_admin),
    q: str | None = None,
    reg_from: str | None = None,
    reg_to: str | None = None,
    min_bonus: str | None = None,
    max_bonus: str | None = None,
    ref_status: str | None = None,
    min_bookings: str | None = None,
    max_bookings: str | None = None,
):
    # Преобразуем пустые строки в None и конвертируем в int
    min_bonus_int = int(min_bonus) if min_bonus and min_bonus.strip() else None
    max_bonus_int = int(max_bonus) if max_bonus and max_bonus.strip() else None
    min_bookings_int = int(min_bookings) if min_bookings and min_bookings.strip() else None
    max_bookings_int = int(max_bookings) if max_bookings and max_bookings.strip() else None
    """
    Реестр пользователей с поиском и фильтрами.
    """
    from sqlalchemy.orm import aliased
    from datetime import date as date_type

    # Подзапрос для подсчёта записей пользователя
    bookings_subq = (
        select(
            BookingEvent.user_id,
            func.count(BookingEvent.id).label("bookings_count")
        )
        .where(BookingEvent.user_id.is_not(None))
        .group_by(BookingEvent.user_id)
        .subquery()
    )

    # Основной запрос с LEFT JOIN на подзапрос записей
    base_query = (
        select(
            User,
            UserBonus,
            func.coalesce(bookings_subq.c.bookings_count, 0).label("bookings_count")
        )
        .join(UserBonus, UserBonus.user_id == User.id, isouter=True)
        .join(bookings_subq, bookings_subq.c.user_id == User.id, isouter=True)
    )

    # Фильтр по поиску
    if q:
        pattern = f"%{q.strip()}%"
        base_query = base_query.where(
            (User.full_name.ilike(pattern)) |
            (User.username.ilike(pattern)) |
            (User.phone.ilike(pattern)) |
            (cast(User.telegram_id, String).ilike(pattern))
        )

    # Фильтр по дате регистрации
    if reg_from:
        try:
            # Пробуем формат d.m.Y
            if '.' in reg_from:
                dt_from = datetime.strptime(reg_from, '%d.%m.%Y').date()
            else:
                dt_from = date_type.fromisoformat(reg_from)
            base_query = base_query.where(User.created_at >= datetime.combine(dt_from, datetime.min.time()))
        except ValueError:
            pass

    if reg_to:
        try:
            # Пробуем формат d.m.Y
            if '.' in reg_to:
                dt_to = datetime.strptime(reg_to, '%d.%m.%Y').date()
            else:
                dt_to = date_type.fromisoformat(reg_to)
            base_query = base_query.where(User.created_at <= datetime.combine(dt_to, datetime.max.time()))
        except ValueError:
            pass

    # Фильтр по бонусам
    if min_bonus_int is not None:
        base_query = base_query.where(func.coalesce(UserBonus.balance, 0) >= min_bonus_int)

    if max_bonus_int is not None:
        base_query = base_query.where(func.coalesce(UserBonus.balance, 0) <= max_bonus_int)

    # Фильтр по рефералам
    if ref_status == "with":
        base_query = base_query.where(UserBonus.referral_earned > 0)
    elif ref_status == "without":
        base_query = base_query.where(
            (UserBonus.referral_earned == 0) | (UserBonus.referral_earned.is_(None))
        )

    # Фильтр по количеству записей
    if min_bookings_int is not None:
        base_query = base_query.where(
            func.coalesce(bookings_subq.c.bookings_count, 0) >= min_bookings_int
        )

    if max_bookings_int is not None:
        base_query = base_query.where(
            func.coalesce(bookings_subq.c.bookings_count, 0) <= max_bookings_int
        )

    q_stmt = (
        base_query
        .order_by(User.created_at.desc())
        .limit(300)
    )

    res = await db.execute(q_stmt)
    rows = res.all()

    return templates.TemplateResponse(
        "users.html",
        {
            "request": request,
            "rows": rows,
            "q": q,
            "reg_from": reg_from,
            "reg_to": reg_to,
            "min_bonus": min_bonus,
            "max_bonus": max_bonus,
            "ref_status": ref_status,
            "min_bookings": min_bookings,
            "max_bookings": max_bookings,
        },
    )


# =========================
#   BROADCAST / РАССЫЛКА УВЕДОМЛЕНИЙ
# =========================

@app.get("/users/broadcast", response_class=HTMLResponse, name="users_broadcast_page")
async def users_broadcast_page(
    request: Request,
    db: AsyncSession = Depends(get_db),
    current_admin: str = Depends(get_current_admin),
    q: str | None = None,
):
    """
    Страница рассылки уведомлений:
    - форма (message_text),
    - таблица пользователей с чекбоксами,
    - поиск по имени/username/телефону/telegram_id.
    """
    base_query = (
        select(User, UserBonus)
        .join(UserBonus, UserBonus.user_id == User.id, isouter=True)
        .where(User.telegram_id.is_not(None))  # только пользователи с Telegram ID
    )

    if q:
        pattern = f"%{q.strip()}%"
        base_query = base_query.where(
            (User.full_name.ilike(pattern)) |
            (User.username.ilike(pattern)) |
            (User.phone.ilike(pattern)) |
            (cast(User.telegram_id, String).ilike(pattern))
        )

    q_stmt = (
        base_query
        .order_by(User.created_at.desc())
        .limit(500)
    )
    res = await db.execute(q_stmt)
    rows = res.all()

    return templates.TemplateResponse(
        "users_broadcast.html",
        {
            "request": request,
            "rows": rows,
            "error": None,
            "message": None,
            "message_text": None,
        },
    )


@app.get("/users/broadcast/search", response_class=HTMLResponse, name="users_broadcast_search")
async def users_broadcast_search(
    request: Request,
    db: AsyncSession = Depends(get_db),
    current_admin: str = Depends(get_current_admin),
    q: str | None = None,
):
    """
    HTMX-поиск по пользователям для broadcast-страницы.
    Возвращает только tbody с чекбоксами.
    """
    base_query = (
        select(User, UserBonus)
        .join(UserBonus, UserBonus.user_id == User.id, isouter=True)
        .where(User.telegram_id.is_not(None))
    )

    if q:
        pattern = f"%{q.strip()}%"
        base_query = base_query.where(
            (User.full_name.ilike(pattern)) |
            (User.username.ilike(pattern)) |
            (User.phone.ilike(pattern)) |
            (cast(User.telegram_id, String).ilike(pattern))
        )

    q_stmt = (
        base_query
        .order_by(User.created_at.desc())
        .limit(500)
    )
    res = await db.execute(q_stmt)
    rows = res.all()

    return templates.TemplateResponse(
        "users_broadcast_rows.html",
        {
            "request": request,
            "rows": rows,
        },
    )


@app.post("/users/broadcast", response_class=HTMLResponse)
async def users_broadcast_submit(
    request: Request,
    db: AsyncSession = Depends(get_db),
    current_admin: str = Depends(get_current_admin),
    message_text: str = Form(...),
    user_ids: list[int] = Form(default=[]),
):
    """
    Рассылка сообщений:
    - message_text обязателен,
    - хотя бы один выбранный user_id.
    """
    message_text = (message_text or "").strip()
    error = None
    message = None

    if not message_text:
        error = "Текст сообщения обязателен."
    elif not user_ids:
        error = "Нужно выбрать хотя бы одного пользователя галочкой."

    if error:
        # Подгрузим часть пользователей, чтобы форма не была пустой при ошибке
        base_query = (
            select(User, UserBonus)
            .join(UserBonus, UserBonus.user_id == User.id, isouter=True)
            .where(User.telegram_id.is_not(None))
            .order_by(User.created_at.desc())
            .limit(500)
        )
        res = await db.execute(base_query)
        rows = res.all()

        return templates.TemplateResponse(
            "users_broadcast.html",
            {
                "request": request,
                "rows": rows,
                "error": error,
                "message": None,
                "message_text": message_text,
            },
            status_code=400,
        )

    sent_ids: list[int] = []
    failed_ids: list[int] = []
    not_found_ids: list[int] = []

    for uid in user_ids:
        user = await db.get(User, uid)
        if not user:
            not_found_ids.append(uid)
            continue

        if not user.telegram_id:
            failed_ids.append(uid)
            continue

        # Отправляем сообщение
        try:
            await _send_telegram_message(user.telegram_id, message_text)
            sent_ids.append(uid)
        except Exception:
            logger.exception(
                "Failed to send broadcast message: user_id=%s telegram_id=%s",
                uid,
                user.telegram_id,
            )
            failed_ids.append(uid)

    # аудит рассылки
    audit = AuditLog(
        admin_username=current_admin,
        action="BROADCAST_SEND",
        entity_type="broadcast",
        entity_id=0,
        payload=json.dumps(
            {
                "message_text": message_text[:500],  # обрезаем для лога
                "sent_ids": sent_ids,
                "failed_ids": failed_ids,
                "not_found_ids": not_found_ids,
                "total_selected": len(user_ids),
            },
            ensure_ascii=False,
        ),
        created_at=datetime.utcnow(),
    )
    db.add(audit)
    await db.commit()

    base_query = (
        select(User, UserBonus)
        .join(UserBonus, UserBonus.user_id == User.id, isouter=True)
        .where(User.telegram_id.is_not(None))
        .order_by(User.created_at.desc())
        .limit(500)
    )
    res = await db.execute(base_query)
    rows = res.all()

    if sent_ids:
        message = f"Рассылка выполнена. Успешно отправлено: {len(sent_ids)}."
        if failed_ids:
            message += f" Ошибки отправки: {len(failed_ids)}."
        if not_found_ids:
            message += f" Не найдены: {len(not_found_ids)}."
    else:
        error = "Не удалось отправить сообщение ни одному пользователю."

    return templates.TemplateResponse(
        "users_broadcast.html",
        {
            "request": request,
            "rows": rows,
            "error": error,
            "message": message if not error else None,
            "message_text": None,
        },
    )


@app.post("/sync-balances", name="sync_balances")
async def sync_balances(
    request: Request,
    db: AsyncSession = Depends(get_db),
    current_admin: str = Depends(get_current_admin),
):
    """
    Синхронизирует балансы всех пользователей из YClients.
    Делает параллельные запросы с ограничением.
    Возвращает JSON с результатами синхронизации.
    """
    import asyncio
    from fastapi.responses import JSONResponse
    
    # Получаем всех пользователей с телефоном
    result = await db.execute(
        select(User).where(User.phone.isnot(None))
    )
    users = result.scalars().all()
    
    synced = 0
    errors = 0
    
    # Синхронизируем батчами по 5 пользователей
    batch_size = 5
    for i in range(0, len(users), batch_size):
        batch = users[i:i + batch_size]
        tasks = [sync_bonus_from_yclients(db, user) for user in batch]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        for r in results:
            if isinstance(r, Exception):
                errors += 1
            elif r is not None:
                synced += 1
        
        # Небольшая пауза между батчами
        if i + batch_size < len(users):
            await asyncio.sleep(0.1)
    
    # Возвращаем JSON с результатами
    return JSONResponse({
        "success": True,
        "synced": synced,
        "errors": errors,
        "total": len(users)
    })


@app.get("/users/{user_id}", response_class=HTMLResponse, name="user_detail")
async def user_detail(
    user_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
    current_admin: str = Depends(get_current_admin),
):
    from datetime import date as date_type

    user = await db.get(User, user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    # Синхронизируем баланс ИЗ YClients (если клиент потратил бонусы в салоне)
    await sync_bonus_from_yclients(db, user)

    bonus_res = await db.execute(
        select(UserBonus).where(UserBonus.user_id == user_id)
    )
    bonus = bonus_res.scalar_one_or_none()

    # История записей из YCLIENTS (если есть yclients_client_id)
    yclients_records = []
    if user.yclients_client_id:
        try:
            yclients = YClientsClient()
            # Получаем записи за последние 90 дней и будущие 30 дней
            today = date_type.today()
            start = today - timedelta(days=90)
            end = today + timedelta(days=30)
            yclients_records = await yclients.get_all_records(
                start_date=start,
                end_date=end,
                count=100,
            )
            # Фильтруем только записи этого клиента
            yclients_records = [
                r for r in yclients_records
                if (r.get("client") or {}).get("id") == user.yclients_client_id
            ]
            # Сортируем по дате (новые сверху)
            yclients_records.sort(
                key=lambda x: x.get("datetime") or x.get("date") or "",
                reverse=True
            )
        except Exception as e:
            logger.warning("Failed to fetch YCLIENTS records for user_id=%s: %s", user_id, e)
    
    # Получаем записи из BookingEvent (записи через бота)
    # Включаем все типы событий, чтобы показать все записи
    booking_events_res = await db.execute(
        select(BookingEvent)
        .where(BookingEvent.user_id == user_id)
        .order_by(BookingEvent.created_at.desc())
        .limit(100)
    )
    booking_events = booking_events_res.scalars().all()
    
    # Создаём словарь для быстрого поиска записей YClients по record_id из meta
    yclients_records_by_id = {}
    for record in yclients_records:
        record_id = record.get("id")
        if record_id:
            yclients_records_by_id[record_id] = record
    
    # Объединяем записи: сначала из YClients, затем из BookingEvent (если их нет в YClients)
    all_records = []
    processed_yclients_ids = set()
    
    # Добавляем записи из YClients
    for record in yclients_records:
        record_id = record.get("id")
        if record_id:
            processed_yclients_ids.add(record_id)
        all_records.append({
            "source": "yclients",
            "data": record,
            "datetime": record.get("datetime") or record.get("date") or "",
        })
    
    # Добавляем записи из BookingEvent, которых нет в YClients
    for event in booking_events:
        record_id = None
        if event.meta and isinstance(event.meta, dict):
            record_id = event.meta.get("record_id")
        
        # Если запись уже есть в YClients, пропускаем
        if record_id and record_id in processed_yclients_ids:
            continue
        
        # Создаём запись из BookingEvent
        event_record = {
            "source": "bot",
            "event_type": event.event_type.value,
            "created_at": event.created_at.isoformat() if event.created_at else "",
            "meta": event.meta or {},
        }
        all_records.append({
            "source": "bot",
            "data": event_record,
            "datetime": event.created_at.isoformat() if event.created_at else "",
        })
    
    # Сортируем все записи по дате (новые сверху)
    all_records.sort(
        key=lambda x: x.get("datetime") or "",
        reverse=True
    )

    # Заявки на бонус за отзывы
    review_requests_res = await db.execute(
        select(ReviewBonusRequest)
        .where(ReviewBonusRequest.user_id == user_id)
        .order_by(ReviewBonusRequest.created_at.desc())
    )
    review_requests = review_requests_res.scalars().all()

    # Количество записей через бота
    bookings_count = await db.scalar(
        select(func.count(BookingEvent.id)).where(BookingEvent.user_id == user_id)
    ) or 0

    return templates.TemplateResponse(
        "user_detail.html",
        {
            "request": request,
            "user": user,
            "bonus": bonus,
            "error": None,
            "yclients_records": [r["data"] for r in all_records],
            "all_records": all_records,
            "review_requests": review_requests,
            "bookings_count": bookings_count,
        },
    )


# Ручные операции по бонусам (начисление/списание)
@app.post("/users/{user_id}/bonus/manual", response_class=HTMLResponse, name="user_manual_bonus")
async def user_manual_bonus(
    user_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
    current_admin: str = Depends(get_current_admin),
    operation: str = Form(...),  # "accrual" | "writeoff"
    amount: int = Form(...),
    comment: str = Form(...),
):
    user = await db.get(User, user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    bonus_res = await db.execute(
        select(UserBonus).where(UserBonus.user_id == user_id)
    )
    bonus = bonus_res.scalar_one_or_none()

    if not bonus:
        # на всякий случай создаём кошелёк, если его ещё нет
        bonus = await get_or_create_user_bonus(db, user)

    # валидация
    if amount <= 0:
        error = "Сумма должна быть положительным числом."
    elif not comment or not comment.strip():
        error = "Комментарий обязателен для любых ручных операций."
    elif operation not in ("accrual", "writeoff"):
        error = "Неизвестный тип операции."
    elif operation == "writeoff" and bonus.balance < amount:
        error = "Недостаточно бонусов: списание не может увести баланс в минус."
    else:
        error = None

    if error:
        return templates.TemplateResponse(
            "user_detail.html",
            {
                "request": request,
                "user": user,
                "bonus": bonus,
                "error": error,
            },
            status_code=400,
        )

    # знак суммы
    delta = amount if operation == "accrual" else -amount

    # транзакция
    tx = BonusTransaction(
        user_id=user.id,
        amount=delta,
        type=BonusTransactionType.ACCRUAL,
        source=BonusTransactionSource.MANUAL,
        created_at=datetime.utcnow(),
        created_by=current_admin,
        comment=comment.strip(),
    )
    db.add(tx)

    # обновляем баланс
    bonus.balance += delta

    user_name_for_log = _user_display_name(user)

    # аудит
    audit_action = "BONUS_MANUAL_ACCRUAL" if delta > 0 else "BONUS_MANUAL_WRITE_OFF"
    audit = AuditLog(
        admin_username=current_admin,
        action=audit_action,
        entity_type="user_bonus",
        entity_id=user.id,
        payload=json.dumps(
            {
                "user_id": user.id,
                "user_name": user_name_for_log,
                "operation": operation,
                "amount": amount,
                "delta": delta,
                "comment": comment.strip(),
                "balance_after": bonus.balance,
            },
            ensure_ascii=False,
        ),
        created_at=datetime.utcnow(),
    )
    db.add(audit)

    await db.commit()
    await db.refresh(bonus)

    # Синхронизируем баланс в YClients
    await sync_bonus_to_yclients(user, bonus.balance, delta=delta)

    # Уведомление пользователю — только при начислении
    if operation == "accrual" and getattr(user, "telegram_id", None):
        text = (
            f"🎁 Вам начислено {amount} бонусов в Demo Lounge.\n\n"
            f"Комментарий администратора: {comment.strip()}"
        )
        await _send_telegram_message(user.telegram_id, text)

    # После успешной операции — редирект на GET запрос (POST-Redirect-GET pattern)
    return RedirectResponse(
        url=request.url_for("user_detail", user_id=user_id),
        status_code=303,  # 303 See Other - для POST запросов
    )


# =========================
#   МАССОВОЕ НАЧИСЛЕНИЕ БОНУСОВ (bulk)
# =========================

@app.get("/users/bonus/bulk", response_class=HTMLResponse, name="users_bulk_bonus_page")
async def users_bulk_bonus_page(
    request: Request,
    db: AsyncSession = Depends(get_db),
    current_admin: str = Depends(get_current_admin),
    q: str | None = None,
):
    """
    Страница массового начисления:
    - форма (amount, comment),
    - таблица пользователей с чекбоксами,
    - поиск по имени/username/телефону/telegram_id.
    """
    base_query = (
        select(User, UserBonus)
        .join(UserBonus, UserBonus.user_id == User.id, isouter=True)
    )

    if q:
        pattern = f"%{q.strip()}%"
        base_query = base_query.where(
            (User.full_name.ilike(pattern)) |
            (User.username.ilike(pattern)) |
            (User.phone.ilike(pattern)) |
            (cast(User.telegram_id, String).ilike(pattern))
        )

    q_stmt = (
        base_query
        .order_by(User.created_at.desc())
        .limit(200)
    )
    res = await db.execute(q_stmt)
    rows = res.all()

    return templates.TemplateResponse(
        "users_bulk_bonus.html",
        {
            "request": request,
            "rows": rows,
            "error": None,
            "message": None,
        },
    )


@app.get("/users/bonus/bulk/search", response_class=HTMLResponse, name="users_bulk_bonus_search")
async def users_bulk_bonus_search(
    request: Request,
    db: AsyncSession = Depends(get_db),
    current_admin: str = Depends(get_current_admin),
    q: str | None = None,
):
    """
    HTMX-поиск по пользователям для bulk-страницы.
    Возвращает только tbody с чекбоксами.
    """
    base_query = (
        select(User, UserBonus)
        .join(UserBonus, UserBonus.user_id == User.id, isouter=True)
    )

    if q:
        pattern = f"%{q.strip()}%"
        base_query = base_query.where(
            (User.full_name.ilike(pattern)) |
            (User.username.ilike(pattern)) |
            (User.phone.ilike(pattern)) |
            (cast(User.telegram_id, String).ilike(pattern))
        )

    q_stmt = (
        base_query
        .order_by(User.created_at.desc())
        .limit(200)
    )
    res = await db.execute(q_stmt)
    rows = res.all()

    return templates.TemplateResponse(
        "users_bulk_rows.html",
        {
            "request": request,
            "rows": rows,
        },
    )


@app.post("/users/bonus/bulk", response_class=HTMLResponse)
async def users_bulk_bonus_submit(
    request: Request,
    db: AsyncSession = Depends(get_db),
    current_admin: str = Depends(get_current_admin),
    amount: int = Form(...),
    comment: str = Form(...),
    user_ids: list[int] = Form(default=[]),
):
    """
    Массовое начисление:
    - amount > 0,
    - comment обязателен,
    - хотя бы один выбранный user_id.
    """
    comment = (comment or "").strip()
    error = None
    message = None

    if amount <= 0:
        error = "Сумма должна быть положительным числом."
    elif not comment:
        error = "Комментарий обязателен для массового начисления."
    elif not user_ids:
        error = "Нужно выбрать хотя бы одного пользователя галочкой."

    if error:
        # Подгрузим часть пользователей, чтобы форма не была пустой при ошибке
        base_query = (
            select(User, UserBonus)
            .join(UserBonus, UserBonus.user_id == User.id, isouter=True)
            .order_by(User.created_at.desc())
            .limit(200)
        )
        res = await db.execute(base_query)
        rows = res.all()

        return templates.TemplateResponse(
            "users_bulk_bonus.html",
            {
                "request": request,
                "rows": rows,
                "error": error,
                "message": None,
            },
            status_code=400,
        )

    processed_ids: list[int] = []
    not_found_ids: list[int] = []

    for uid in user_ids:
        user = await db.get(User, uid)
        if not user:
            not_found_ids.append(uid)
            continue

        bonus_res = await db.execute(
            select(UserBonus).where(UserBonus.user_id == uid)
        )
        bonus = bonus_res.scalar_one_or_none()
        if not bonus:
            bonus = await get_or_create_user_bonus(db, user)

        # транзакция
        tx = BonusTransaction(
            user_id=user.id,
            amount=amount,
            type=BonusTransactionType.ACCRUAL,
            source=BonusTransactionSource.MANUAL,
            created_at=datetime.utcnow(),
            created_by=current_admin,
            comment=f"[bulk] {comment}",
        )
        db.add(tx)

        bonus.balance += amount
        processed_ids.append(uid)

        # уведомление пользователю
        if getattr(user, "telegram_id", None):
            text = (
            f"🎁 Вам начислено {amount} бонусов в Demo Lounge.\n\n"
                f"Комментарий администратора: {comment}"
            )
            await _send_telegram_message(user.telegram_id, text)
        
        # Синхронизируем баланс в YClients (внутри цикла для каждого пользователя)
        await sync_bonus_to_yclients(user, bonus.balance, delta=amount)

    # аудит массовой операции
    audit = AuditLog(
        admin_username=current_admin,
        action="BONUS_MANUAL_BULK_ACCRUAL",
        entity_type="user_bonus_bulk",
        entity_id=0,
        payload=json.dumps(
            {
                "amount": amount,
                "comment": comment,
                "processed_ids": processed_ids,
                "not_found_ids": not_found_ids,
            },
            ensure_ascii=False,
        ),
        created_at=datetime.utcnow(),
    )
    db.add(audit)

    await db.commit()

    base_query = (
        select(User, UserBonus)
        .join(UserBonus, UserBonus.user_id == User.id, isouter=True)
        .order_by(User.created_at.desc())
        .limit(200)
    )
    res = await db.execute(base_query)
    rows = res.all()

    if processed_ids:
        message = f"Начисление выполнено. Успешно: {len(processed_ids)}. Не найдены: {len(not_found_ids)}."
    else:
        error = "Не удалось начислить бонусы ни одному пользователю."

    return templates.TemplateResponse(
        "users_bulk_bonus.html",
        {
            "request": request,
            "rows": rows,
            "error": error,
            "message": message if not error else None,
        },
    )
# =========================
#   BOOKINGS / УПРАВЛЕНИЕ ЗАПИСЯМИ YCLIENTS
# =========================

@app.get("/bookings", response_class=HTMLResponse, name="bookings_list")
async def bookings_list(
    request: Request,
    current_admin: str = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db),
    start_date: str | None = None,
    end_date: str | None = None,
    q: str | None = None,
):
    """
    Список записей из YCLIENTS за выбранный период.
    """
    from datetime import date as date_type
    from bot.yclients_client import YClientsClient

    # Парсим даты
    today = date_type.today()
    try:
        if start_date:
            # Пробуем формат d.m.Y
            if '.' in start_date:
                start = datetime.strptime(start_date, '%d.%m.%Y').date()
            else:
                start = date_type.fromisoformat(start_date)
        else:
            start = today
    except ValueError:
        start = today

    try:
        if end_date:
            # Пробуем формат d.m.Y
            if '.' in end_date:
                end = datetime.strptime(end_date, '%d.%m.%Y').date()
            else:
                end = date_type.fromisoformat(end_date)
        else:
            end = today + timedelta(days=14)
    except ValueError:
        end = today + timedelta(days=14)

    # Получаем записи из YCLIENTS
    yclients = YClientsClient()
    records = await yclients.get_all_records(
        start_date=start, end_date=end, count=200, include_deleted=True
    )

    # Оставляем только клиентов бота (по yclients_client_id или телефону)
    bot_yc_ids: set[int] = set()
    bot_phones: set[str] = set()
    users_res = await db.execute(select(User.phone, User.yclients_client_id))
    for phone, yc_id in users_res.all():
        norm = _normalize_phone(phone)
        if norm:
            bot_phones.add(norm)
        if yc_id:
            try:
                bot_yc_ids.add(int(yc_id))
            except Exception:
                continue

    filtered_bot_records = []
    for r in records:
        client = r.get("client") or {}
        cid = client.get("id")
        phone_raw = client.get("phone") or r.get("phone") or r.get("client_phone") or ""
        norm = _normalize_phone(phone_raw)
        if (cid and cid in bot_yc_ids) or (norm and norm in bot_phones):
            filtered_bot_records.append(r)
    records = filtered_bot_records

    # Фильтрация по поиску (имя клиента или телефон)
    if q:
        q_lower = q.lower().strip()
        filtered = []
        for r in records:
            client = r.get("client") or {}
            client_name = (client.get("name") or client.get("full_name") or "").lower()
            client_phone = (client.get("phone") or "").lower()
            if q_lower in client_name or q_lower in client_phone:
                filtered.append(r)
        records = filtered

    # Сортируем по дате (ближайшие сверху)
    def get_dt(rec):
        dt_str = rec.get("datetime") or rec.get("date") or ""
        return dt_str

    records.sort(key=get_dt)

    return templates.TemplateResponse(
        "bookings.html",
        {
            "request": request,
            "records": records,
            "start_date": start.isoformat(),
            "end_date": end.isoformat(),
            "q": q,
            "error": None,
            "message": None,
        },
    )


@app.post("/bookings/{record_id}/cancel", name="booking_cancel")
async def booking_cancel(
    record_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
    current_admin: str = Depends(get_current_admin),
):
    """
    Отмена записи через YCLIENTS API.
    """
    from bot.yclients_client import YClientsClient

    yclients = YClientsClient()

    # Получаем информацию о записи для логирования
    record = await yclients.get_record_by_id(record_id)
    record_info = {}
    if record:
        client = record.get("client") or {}
        staff = record.get("staff") or {}
        phone_raw = client.get("phone") or record.get("phone") or record.get("client_phone") or ""
        norm_phone = _normalize_phone(phone_raw)
        yc_client_id = client.get("id")

        user_for_log = None
        if norm_phone:
            user_for_log = await db.scalar(select(User).where(User.phone == norm_phone))
        if not user_for_log and yc_client_id:
            user_for_log = await get_user_by_yclients_id(db, yc_client_id)

        record_info = {
            "record_id": record_id,
            "datetime": record.get("datetime") or record.get("date"),
            "client_name": client.get("name") or client.get("full_name"),
            "client_phone": client.get("phone"),
            "staff_name": staff.get("name"),
            "yclients_client_id": yc_client_id,
            "services": record.get("services") or [],
        }

        # Логируем отмену локально (без дублей)
        if user_for_log:
            exists_cancel = await db.scalar(
                select(func.count(BookingEvent.id)).where(
                    BookingEvent.event_type == BookingEventType.CANCELLED,
                    cast(BookingEvent.meta["record_id"].astext, String) == str(record_id),
                )
            )
            if not exists_cancel:
                try:
                    await log_booking_event(
                        session=db,
                        user=user_for_log,
                        event_type=BookingEventType.CANCELLED,
                        yclients_record_id=record_id,
                        meta=record_info,
                    )
                except Exception:
                    logger.exception("Failed to log cancellation locally", extra={"record_id": record_id})

    # Удаляем запись
    success = await yclients.delete_record(record_id)

    # Логируем в аудит
    audit = AuditLog(
        admin_username=current_admin,
        action="BOOKING_CANCEL",
        entity_type="yclients_record",
        entity_id=record_id,
        payload=json.dumps(
            {
                "success": success,
                **record_info,
            },
            ensure_ascii=False,
        ),
        created_at=datetime.utcnow(),
    )
    db.add(audit)
    await db.commit()

    if success:
        logger.info(
            "Booking cancelled by admin: record_id=%s admin=%s",
            record_id,
            current_admin,
        )
        return RedirectResponse(
            url=str(request.url_for("bookings_list")) + "?message=cancelled",
            status_code=302,
        )
    else:
        logger.warning(
            "Booking cancel failed: record_id=%s admin=%s",
            record_id,
            current_admin,
        )
        return RedirectResponse(
            url=str(request.url_for("bookings_list")) + "?error=cancel_failed",
            status_code=302,
        )


# =========================
#   REVIEWS / ОТЗЫВЫ
# =========================

@app.get("/reviews", response_class=HTMLResponse, name="reviews_list")
async def reviews_list(
    request: Request,
    db: AsyncSession = Depends(get_db),
    current_admin: str = Depends(get_current_admin),
    status_filter: str | None = None,
    platform_filter: str | None = None,
):
    # EAGER-LOAD user, чтобы в шаблоне не было ленивой загрузки (и не падало с MissingGreenlet)
    query = (
        select(ReviewBonusRequest)
        .options(selectinload(ReviewBonusRequest.user))
        .order_by(ReviewBonusRequest.created_at.desc())
    )

    status_enum = None
    if status_filter:
        try:
            status_enum = ReviewRequestStatus(status_filter)
            query = query.where(ReviewBonusRequest.status == status_enum)
        except ValueError:
            status_enum = None

    # Фильтр по платформе
    if platform_filter in ("yandex", "2gis"):
        query = query.where(ReviewBonusRequest.platform == platform_filter)

    res = await db.execute(query)
    reviews = res.scalars().all()

    # Найти пользователя с максимальным количеством отзывов
    top_reviewer_query = (
        select(ReviewBonusRequest.user_id, func.count(ReviewBonusRequest.id).label("cnt"))
        .group_by(ReviewBonusRequest.user_id)
        .order_by(func.count(ReviewBonusRequest.id).desc())
        .limit(1)
    )
    top_res = await db.execute(top_reviewer_query)
    top_row = top_res.first()
    top_reviewer_id = top_row[0] if top_row and top_row[1] > 1 else None

    return templates.TemplateResponse(
        "reviews_list.html",
        {
            "request": request,
            "reviews": reviews,
            "status_filter": status_enum,
            "platform_filter": platform_filter,
            "ReviewRequestStatus": ReviewRequestStatus,
            "top_reviewer_id": top_reviewer_id,
        },
    )


@app.get("/reviews/{review_id}", response_class=HTMLResponse, name="review_detail")
async def review_detail(
    review_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
    current_admin: str = Depends(get_current_admin),
):
    # тоже EAGER-LOAD user
    res = await db.execute(
        select(ReviewBonusRequest)
        .options(selectinload(ReviewBonusRequest.user))
        .where(ReviewBonusRequest.id == review_id)
    )
    review = res.scalars().first()
    if not review:
        raise HTTPException(status_code=404, detail="Review not found")

    return templates.TemplateResponse(
        "review_detail.html",
        {
            "request": request,
            "review": review,
        },
    )


# Картинка отзыва: кешируем на диске и отдаём с сервера
@app.get("/reviews/{review_id}/image")
async def review_image(
    review_id: int,
    db: AsyncSession = Depends(get_db),
    current_admin: str = Depends(get_current_admin),
):
    review = await db.get(ReviewBonusRequest, review_id)
    if not review:
        raise HTTPException(status_code=404, detail="Review not found")

    if not review.image_file_id:
        raise HTTPException(status_code=404, detail="No image for this review")

    # Каталог для кеша картинок
    cache_dir = Path("admin_media/reviews")
    cache_dir.mkdir(parents=True, exist_ok=True)
    img_path = cache_dir / f"review_{review.id}.jpg"

    # Если уже скачивали — просто отдаём с диска
    if img_path.exists():
        return StreamingResponse(img_path.open("rb"), media_type="image/jpeg")

    # Иначе — один раз тянем из Telegram и сохраняем
    token = os.getenv("BOT_TOKEN")
    if not token:
        raise HTTPException(status_code=500, detail="BOT_TOKEN is not configured in environment")

    bot = Bot(token)
    try:
        tg_file = await bot.get_file(review.image_file_id)
        await bot.download_file(tg_file.file_path, destination=img_path)
    except Exception as e:
        await bot.session.close()
        logger.exception("Error downloading image from Telegram for review_id=%s", review.id)
        return JSONResponse(
            status_code=500,
            content={"detail": f"Error downloading image from Telegram: {e}"},
        )
    await bot.session.close()

    return StreamingResponse(img_path.open("rb"), media_type="image/jpeg")


# =========================
#   REVIEW CONFIRM / REJECT
# =========================

@app.post("/reviews/{review_id}/confirm")
async def review_confirm(
    review_id: int,
    request: Request,
    comment: str | None = Form(default=None),
    db: AsyncSession = Depends(get_db),
    current_admin: str = Depends(get_current_admin),
):
    # грузим заявку + пользователя
    res = await db.execute(
        select(ReviewBonusRequest)
        .options(selectinload(ReviewBonusRequest.user))
        .where(ReviewBonusRequest.id == review_id)
    )
    review = res.scalars().first()
    if not review:
        raise HTTPException(status_code=404, detail="Review not found")

    if review.status != ReviewRequestStatus.NEW:
        return RedirectResponse(url=f"/reviews/{review_id}", status_code=302)

    user = review.user
    if not user:
        raise HTTPException(status_code=400, detail="Review has no linked user")

    # берём/создаём бонусный кошелёк
    bonus = await get_or_create_user_bonus(db, user)

    # сумма из глобального конфига
    config = await get_bonus_config(db)
    amount = config.review_amount

    # транзакция по бонусам
    tx = BonusTransaction(
        user_id=user.id,
        amount=amount,
        type=BonusTransactionType.ACCRUAL,
        source=BonusTransactionSource.REVIEW,
        created_at=datetime.utcnow(),
        created_by=current_admin,
        comment=comment or "Бонус за отзыв (подтвержден через админку)",
    )
    db.add(tx)

    # начисляем на баланс
    bonus.balance += amount
    
    # устанавливаем флаг выполнения задания по отзыву
    if review.platform == "yandex":
        bonus.review_yandex_given = True
    elif review.platform == "2gis":
        bonus.review_2gis_given = True

    # помечаем заявку
    review.status = ReviewRequestStatus.CONFIRMED
    review.decided_at = datetime.utcnow()
    review.decided_by = current_admin
    review.decision_comment = comment
    await db.flush()
    review.bonus_transaction_id = tx.id

    user_name_for_log = _user_display_name(user)

    # лог в аудит
    audit = AuditLog(
        admin_username=current_admin,
        action="REVIEW_CONFIRM",
        entity_type="review_request",
        entity_id=review.id,
        payload=json.dumps(
            {
                "user_id": user.id,
                "user_name": user_name_for_log,
                "amount": amount,
                "comment": comment,
            },
            ensure_ascii=False,
        ),
        created_at=datetime.utcnow(),
    )
    db.add(audit)

    await db.commit()
    await db.refresh(bonus)

    # Синхронизируем баланс в YClients
    await sync_bonus_to_yclients(user, bonus.balance, delta=amount)

    # Уведомление пользователю
    chat_id = _get_review_chat_id(review, user)
    if chat_id:
        text = (
            f"🎁 Вам начислено {amount} бонусов за отзыв 🔥\n\n"
            f"Спасибо за отзыв о Demo Lounge!"
        )
        await _send_telegram_message(chat_id, text)

    return RedirectResponse(url=f"/reviews/{review_id}", status_code=302)


@app.post("/reviews/{review_id}/reject")
async def review_reject(
    review_id: int,
    request: Request,
    comment: str | None = Form(default=None),
    db: AsyncSession = Depends(get_db),
    current_admin: str = Depends(get_current_admin),
):
    res = await db.execute(
        select(ReviewBonusRequest)
        .where(ReviewBonusRequest.id == review_id)
    )
    review = res.scalars().first()
    if not review:
        raise HTTPException(status_code=404, detail="Review not found")

    if review.status != ReviewRequestStatus.NEW:
        return RedirectResponse(url=f"/reviews/{review_id}", status_code=302)

    review.status = ReviewRequestStatus.REJECTED
    review.decided_at = datetime.utcnow()
    review.decided_by = current_admin
    review.decision_comment = comment

    audit = AuditLog(
        admin_username=current_admin,
        action="REVIEW_REJECT",
        entity_type="review_request",
        entity_id=review.id,
        payload=json.dumps(
            {
                "comment": comment,
            },
            ensure_ascii=False,
        ),
        created_at=datetime.utcnow(),
    )
    db.add(audit)

    await db.commit()

    # Уведомление об отказе
    chat_id = _get_review_chat_id(review, None)
    if chat_id:
        base_text = "❌ Заявка на бонус за отзыв отклонена."
        if comment:
            text = f"{base_text}\n\nКомментарий администратора: {comment}"
        else:
            text = f"{base_text}\n\nЕсли есть вопросы — напишите администратору салона."
        await _send_telegram_message(chat_id, text)

    return RedirectResponse(url=f"/reviews/{review_id}", status_code=302)


# =========================
#   QUICK REVIEW ACTIONS (HTMX)
# =========================

@app.post("/reviews/{review_id}/confirm/quick", response_class=HTMLResponse)
async def review_confirm_quick(
    review_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
    current_admin: str = Depends(get_current_admin),
):
    """Быстрое подтверждение отзыва через HTMX — возвращает обновлённую строку таблицы."""
    res = await db.execute(
        select(ReviewBonusRequest)
        .options(selectinload(ReviewBonusRequest.user))
        .where(ReviewBonusRequest.id == review_id)
    )
    review = res.scalars().first()
    if not review:
        raise HTTPException(status_code=404, detail="Review not found")

    if review.status != ReviewRequestStatus.NEW:
        # Уже обработан — просто вернём строку
        return templates.TemplateResponse(
            "review_row.html",
            {"request": request, "r": review, "top_reviewer_id": None},
        )

    user = review.user
    if not user:
        raise HTTPException(status_code=400, detail="Review has no linked user")

    bonus = await get_or_create_user_bonus(db, user)
    config = await get_bonus_config(db)
    amount = config.review_amount

    tx = BonusTransaction(
        user_id=user.id,
        amount=amount,
        type=BonusTransactionType.ACCRUAL,
        source=BonusTransactionSource.REVIEW,
        created_at=datetime.utcnow(),
        created_by=current_admin,
        comment="Бонус за отзыв (быстрое подтверждение)",
    )
    db.add(tx)

    bonus.balance += amount
    if review.platform == "yandex":
        bonus.review_yandex_given = True
    elif review.platform == "2gis":
        bonus.review_2gis_given = True

    review.status = ReviewRequestStatus.CONFIRMED
    review.decided_at = datetime.utcnow()
    review.decided_by = current_admin
    await db.flush()
    review.bonus_transaction_id = tx.id

    audit = AuditLog(
        admin_username=current_admin,
        action="REVIEW_CONFIRM",
        entity_type="review_request",
        entity_id=review.id,
        payload=json.dumps({"user_id": user.id, "amount": amount, "quick": True}, ensure_ascii=False),
        created_at=datetime.utcnow(),
    )
    db.add(audit)
    await db.commit()
    await db.refresh(bonus)

    # Синхронизируем баланс в YClients
    await sync_bonus_to_yclients(user, bonus.balance, delta=amount)

    # Уведомление пользователю
    chat_id = _get_review_chat_id(review, user)
    if chat_id:
        text = f"🎁 Вам начислено {amount} бонусов за отзыв 🔥\n\nСпасибо за отзыв о Demo Lounge!"
        await _send_telegram_message(chat_id, text)

    return templates.TemplateResponse(
        "review_row.html",
        {"request": request, "r": review, "top_reviewer_id": None},
    )


@app.post("/reviews/{review_id}/reject/quick", response_class=HTMLResponse)
async def review_reject_quick(
    review_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
    current_admin: str = Depends(get_current_admin),
):
    """Быстрое отклонение отзыва через HTMX — возвращает обновлённую строку таблицы."""
    res = await db.execute(
        select(ReviewBonusRequest)
        .options(selectinload(ReviewBonusRequest.user))
        .where(ReviewBonusRequest.id == review_id)
    )
    review = res.scalars().first()
    if not review:
        raise HTTPException(status_code=404, detail="Review not found")

    if review.status != ReviewRequestStatus.NEW:
        return templates.TemplateResponse(
            "review_row.html",
            {"request": request, "r": review, "top_reviewer_id": None},
        )

    review.status = ReviewRequestStatus.REJECTED
    review.decided_at = datetime.utcnow()
    review.decided_by = current_admin

    audit = AuditLog(
        admin_username=current_admin,
        action="REVIEW_REJECT",
        entity_type="review_request",
        entity_id=review.id,
        payload=json.dumps({"quick": True}, ensure_ascii=False),
        created_at=datetime.utcnow(),
    )
    db.add(audit)
    await db.commit()

    # Уведомление об отказе
    chat_id = _get_review_chat_id(review, None)
    if chat_id:
        text = "❌ Заявка на бонус за отзыв отклонена.\n\nЕсли есть вопросы — напишите администратору салона."
        await _send_telegram_message(chat_id, text)

    return templates.TemplateResponse(
        "review_row.html",
        {"request": request, "r": review, "top_reviewer_id": None},
    )


# =========================
#   USER BONUS HISTORY
# =========================

@app.get("/users/{user_id}/bonus", response_class=HTMLResponse, name="user_bonus_history")
async def user_bonus_history(
    user_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
    current_admin: str = Depends(get_current_admin),
):
    user = await db.get(User, user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    res = await db.execute(
        select(BonusTransaction)
        .where(BonusTransaction.user_id == user_id)
        .order_by(BonusTransaction.created_at.desc())
    )
    transactions = res.scalars().all()

    return templates.TemplateResponse(
        "user_bonus_history.html",
        {
            "request": request,
            "user": user,
            "transactions": transactions,
        },
    )


# =========================
#   AUDIT LOG
# =========================

@app.get("/audit", response_class=HTMLResponse, name="audit_log")
async def audit_log(
    request: Request,
    db: AsyncSession = Depends(get_db),
    current_admin: str = Depends(get_current_admin),
    action: str | None = None,
    admin: str | None = None,
    q: str | None = None,
):
    """
    Просмотр последних событий AuditLog с простыми фильтрами.
    """
    query = select(AuditLog).order_by(AuditLog.created_at.desc())

    if action:
        query = query.where(AuditLog.action == action)

    if admin:
        pattern_admin = f"%{admin.strip()}%"
        query = query.where(AuditLog.admin_username.ilike(pattern_admin))

    if q:
        pattern = f"%{q.strip()}%"
        query = query.where(
            (AuditLog.payload.ilike(pattern)) |
            (AuditLog.entity_type.ilike(pattern)) |
            (cast(AuditLog.entity_id, String).ilike(pattern))
        )

    query = query.limit(300)

    res = await db.execute(query)
    logs = res.scalars().all()

    # Список доступных типов действий для фильтра
    actions_res = await db.execute(
        select(AuditLog.action)
        .distinct()
        .order_by(AuditLog.action)
    )
    actions = [row[0] for row in actions_res.all()]
    actions_with_labels = [(act, _human_action_label(act or "")) for act in actions]

    # Обогащаем записи человекочитаемыми полями
    for log in logs:
        log.human_action = _human_action_label(log.action or "")
        log.human_entity = _human_entity_label(log.entity_type, log.entity_id)
        log.human_details = _human_payload_details(
            log.action or "",
            log.payload,
            log.entity_type,
            log.entity_id,
        )

    return templates.TemplateResponse(
        "audit.html",
        {
            "request": request,
            "logs": logs,
            "actions": actions,
            "actions_with_labels": actions_with_labels,
            "current_action": action,
            "admin_filter": admin,
            "q": q,
        },
    )


# =========================
#   SETTINGS · ГЛОБАЛЬНЫЕ НАСТРОЙКИ БОНУСОВ
# =========================

@app.get("/settings", response_class=HTMLResponse, name="settings_page")
async def settings_page(
    request: Request,
    db: AsyncSession = Depends(get_db),
    current_admin: str = Depends(get_current_admin),
):
    config = await get_bonus_config(db)
    return templates.TemplateResponse(
        "settings.html",
        {
            "request": request,
            "config": config,
            "message": None,
        },
    )


@app.post("/settings", response_class=HTMLResponse)
async def settings_submit(
    request: Request,
    db: AsyncSession = Depends(get_db),
    current_admin: str = Depends(get_current_admin),
    welcome_amount: int = Form(...),
    channel_amount: int = Form(...),
    review_amount: int = Form(...),
    referral_amount: int = Form(...),
    welcome_enabled: str | None = Form(default=None),
    channel_enabled: str | None = Form(default=None),
    review_enabled: str | None = Form(default=None),
    referral_enabled: str | None = Form(default=None),
    max_bonus_percent: int = Form(default=30),
):
    config = await get_bonus_config(db)

    # простая валидация
    for name, value in [
        ("welcome_amount", welcome_amount),
        ("channel_amount", channel_amount),
        ("review_amount", review_amount),
        ("referral_amount", referral_amount),
    ]:
        if value < 0:
            return templates.TemplateResponse(
                "settings.html",
                {
                    "request": request,
                    "config": config,
                    "message": f"Значение {name} не может быть отрицательным",
                },
                status_code=400,
            )

    if max_bonus_percent < 0 or max_bonus_percent > 100:
        return templates.TemplateResponse(
            "settings.html",
            {
                "request": request,
                "config": config,
                "message": "Максимальный % должен быть от 0 до 100",
            },
            status_code=400,
        )

    # Суммы
    config.welcome_amount = welcome_amount
    config.channel_amount = channel_amount
    config.review_amount = review_amount
    config.referral_amount = referral_amount

    # Статусы (checkbox: если есть значение — включено, нет — выключено)
    config.welcome_enabled = welcome_enabled is not None
    config.channel_enabled = channel_enabled is not None
    config.review_enabled = review_enabled is not None
    config.referral_enabled = referral_enabled is not None

    # Правила списания
    config.max_bonus_percent = max_bonus_percent

    config.updated_at = datetime.utcnow()

    await db.commit()
    await db.refresh(config)

    # логируем изменение
    audit = AuditLog(
        admin_username=current_admin,
        action="BONUS_CONFIG_UPDATE",
        entity_type="bonus_config",
        entity_id=config.id,
        payload=json.dumps(
            {
                "welcome_amount": welcome_amount,
                "welcome_enabled": config.welcome_enabled,
                "channel_amount": channel_amount,
                "channel_enabled": config.channel_enabled,
                "review_amount": review_amount,
                "review_enabled": config.review_enabled,
                "referral_amount": referral_amount,
                "referral_enabled": config.referral_enabled,
                "max_bonus_percent": max_bonus_percent,
            },
            ensure_ascii=False,
        ),
        created_at=datetime.utcnow(),
    )
    db.add(audit)
    await db.commit()

    return templates.TemplateResponse(
        "settings.html",
        {
            "request": request,
            "config": config,
            "message": "Настройки сохранены",
        },
    )


# =========================
#   РЕДАКТИРОВАНИЕ ТЕКСТОВ БОТА
# =========================

@app.get("/texts", response_class=HTMLResponse, name="bot_texts_page")
async def bot_texts_page(
    request: Request,
    db: AsyncSession = Depends(get_db),
    current_admin: str = Depends(get_current_admin),
):
    """
    Страница редактирования текстов бота.
    """
    # Инициализируем тексты если их нет
    await init_bot_texts(db)
    
    # Получаем все тексты
    result = await db.execute(select(BotText).order_by(BotText.key))
    texts = result.scalars().all()
    
    return templates.TemplateResponse(
        "bot_texts.html",
        {
            "request": request,
            "texts": texts,
            "message": None,
        },
    )


@app.post("/texts", response_class=HTMLResponse, name="bot_texts_submit")
async def bot_texts_submit(
    request: Request,
    db: AsyncSession = Depends(get_db),
    current_admin: str = Depends(get_current_admin),
):
    """
    Сохранение отредактированных текстов.
    """
    form_data = await request.form()
    
    updated_keys = []
    for key, value in form_data.items():
        if key.startswith("text_"):
            text_key = key[5:]  # убираем префикс "text_"
            result = await db.execute(select(BotText).where(BotText.key == text_key))
            text_row = result.scalar_one_or_none()
            
            if text_row:
                old_value = text_row.value
                text_row.value = value
                text_row.updated_at = datetime.utcnow()
                updated_keys.append(text_key)
                
                # Логируем изменение
                if old_value != value:
                    audit = AuditLog(
                        admin_username=current_admin,
                        action="BOT_TEXT_UPDATE",
                        entity_type="bot_text",
                        entity_id=text_row.id,
                        payload=json.dumps(
                            {
                                "key": text_key,
                                "old_value": old_value[:200] if old_value else None,
                                "new_value": value[:200] if value else None,
                            },
                            ensure_ascii=False,
                        ),
                        created_at=datetime.utcnow(),
                    )
                    db.add(audit)
    
    await db.commit()
    
    # Получаем все тексты для отображения
    result = await db.execute(select(BotText).order_by(BotText.key))
    texts = result.scalars().all()
    
    return templates.TemplateResponse(
        "bot_texts.html",
        {
            "request": request,
            "texts": texts,
            "message": f"Сохранено: {len(updated_keys)} текст(ов)",
        },
    )


# =========================
#   УПРАВЛЕНИЕ ПРОМОКОДАМИ
# =========================

@app.get("/promocodes", response_class=HTMLResponse, name="promocodes_list")
async def promocodes_list(
    request: Request,
    db: AsyncSession = Depends(get_db),
    current_admin: str = Depends(get_current_admin),
):
    """
    Список промокодов.
    """
    result = await db.execute(
        select(Promocode).order_by(Promocode.created_at.desc())
    )
    promocodes = result.scalars().all()
    
    return templates.TemplateResponse(
        "promocodes.html",
        {
            "request": request,
            "promocodes": promocodes,
            "message": None,
            "error": None,
        },
    )


@app.post("/promocodes", response_class=HTMLResponse, name="promocode_create")
async def promocode_create(
    request: Request,
    db: AsyncSession = Depends(get_db),
    current_admin: str = Depends(get_current_admin),
    code: str = Form(...),
    bonus_amount: int = Form(...),
    max_uses: int = Form(default=0),
    description: str = Form(default=""),
):
    """
    Создание нового промокода.
    """
    # Нормализуем код
    code = code.strip().upper()
    
    # Проверяем уникальность
    existing = await db.execute(
        select(Promocode).where(Promocode.code == code)
    )
    if existing.scalar_one_or_none():
        result = await db.execute(
            select(Promocode).order_by(Promocode.created_at.desc())
        )
        promocodes = result.scalars().all()
        return templates.TemplateResponse(
            "promocodes.html",
            {
                "request": request,
                "promocodes": promocodes,
                "message": None,
                "error": f"Промокод {code} уже существует",
            },
        )
    
    promo = Promocode(
        code=code,
        bonus_amount=bonus_amount,
        max_uses=max_uses,
        description=description or None,
    )
    db.add(promo)
    
    # Логируем
    audit = AuditLog(
        admin_username=current_admin,
        action="PROMOCODE_CREATE",
        entity_type="promocode",
        entity_id=0,
        payload=json.dumps(
            {"code": code, "bonus_amount": bonus_amount, "max_uses": max_uses},
            ensure_ascii=False,
        ),
        created_at=datetime.utcnow(),
    )
    db.add(audit)
    
    await db.commit()
    
    result = await db.execute(
        select(Promocode).order_by(Promocode.created_at.desc())
    )
    promocodes = result.scalars().all()
    
    return templates.TemplateResponse(
        "promocodes.html",
        {
            "request": request,
            "promocodes": promocodes,
            "message": f"Промокод {code} создан",
            "error": None,
        },
    )


@app.post("/promocodes/{promo_id}/toggle", response_class=HTMLResponse, name="promocode_toggle")
async def promocode_toggle(
    promo_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
    current_admin: str = Depends(get_current_admin),
):
    """
    Включить/выключить промокод.
    """
    promo = await db.get(Promocode, promo_id)
    if not promo:
        return RedirectResponse(url="/promocodes", status_code=302)
    
    promo.is_active = not promo.is_active
    
    audit = AuditLog(
        admin_username=current_admin,
        action="PROMOCODE_TOGGLE",
        entity_type="promocode",
        entity_id=promo_id,
        payload=json.dumps({"code": promo.code, "is_active": promo.is_active}),
        created_at=datetime.utcnow(),
    )
    db.add(audit)
    
    await db.commit()
    
    return RedirectResponse(url="/promocodes", status_code=302)


@app.post("/promocodes/{promo_id}/delete", response_class=HTMLResponse, name="promocode_delete")
async def promocode_delete(
    promo_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
    current_admin: str = Depends(get_current_admin),
):
    """
    Удалить промокод.
    """
    promo = await db.get(Promocode, promo_id)
    if promo:
        audit = AuditLog(
            admin_username=current_admin,
            action="PROMOCODE_DELETE",
            entity_type="promocode",
            entity_id=promo_id,
            payload=json.dumps({"code": promo.code}),
            created_at=datetime.utcnow(),
        )
        db.add(audit)
        
        # Сначала удаляем связанные записи использования промокода
        await db.execute(
            delete(PromocodeUsage).where(PromocodeUsage.promocode_id == promo_id)
        )
        
        await db.delete(promo)
        await db.commit()
    
    return RedirectResponse(url="/promocodes", status_code=302)


@app.get("/analytics/bookings", response_class=HTMLResponse, name="bookings_analytics")
async def bookings_analytics(
    request: Request,
    db: AsyncSession = Depends(get_db),
    current_admin: str = Depends(get_current_admin),
):
    """
    Аналитика по записям через бота.

    Берём события из BookingEvent за последние 30 дней:
    - CREATED / CLICK_BOOKING — считаем как «создано»;
    - COMPLETED — визит состоялся;
    - CANCELLED — запись отменена.

    Отсюда считаем:
    - агрегаты (карточки);
    - динамику по дням для графика;
    - воронку «клик → запись → визит»;
    - разрез по источникам трафика (meta.source) для завершённых визитов.
    """
    # Живой синк записей при каждом заходе
    try:
        await sync_yclients_records_once()
    except Exception:
        logger.exception("sync_yclients_records_once failed on bookings_analytics")

    period_days = 30
    now = datetime.utcnow()
    since = now - timedelta(days=period_days)

    # ==== Агрегаты по типам событий за период ====
    base_count_q = (
        select(func.count(BookingEvent.id))
        .where(BookingEvent.created_at >= since)
    )

    # Клики по кнопке записи (отдельно для воронки)
    clicks_count = await db.scalar(
        base_count_q.where(BookingEvent.event_type == BookingEventType.CLICK_BOOKING)
    ) or 0

    # Создано записей: только факты записей из CRM (без кликов)
    created_count = await db.scalar(
        base_count_q.where(BookingEvent.event_type == BookingEventType.CREATED)
    ) or 0

    # Завершено визитов
    completed_count = await db.scalar(
        base_count_q.where(BookingEvent.event_type == BookingEventType.COMPLETED)
    ) or 0

    # Отменено записей
    cancelled_count = await db.scalar(
        base_count_q.where(BookingEvent.event_type == BookingEventType.CANCELLED)
    ) or 0

    stats = SimpleNamespace(
        created=int(created_count),
        completed=int(completed_count),
        cancelled=int(cancelled_count),
        period_days=period_days,
    )

    # ==== Динамика по дням ====
    daily_q = (
        select(
            func.date_trunc("day", BookingEvent.created_at).label("day"),
            func.sum(
                case(
                    (BookingEvent.event_type == BookingEventType.CREATED, 1),
                    else_=0,
                )
            ).label("created"),
            func.sum(
                case(
                    (BookingEvent.event_type == BookingEventType.COMPLETED, 1),
                    else_=0,
                )
            ).label("completed"),
            func.sum(
                case(
                    (BookingEvent.event_type == BookingEventType.CANCELLED, 1),
                    else_=0,
                )
            ).label("cancelled"),
        )
        .where(BookingEvent.created_at >= since)
        .group_by("day")
        .order_by("day")
    )

    daily_res = await db.execute(daily_q)
    daily_rows = daily_res.all()

    daily_labels: list[str] = []
    daily_created: list[int] = []
    daily_completed: list[int] = []
    daily_cancelled: list[int] = []

    for day, c_created, c_completed, c_cancelled in daily_rows:
        # day — это datetime с обнулённым временем
        daily_labels.append(day.strftime("%d.%m"))
        daily_created.append(int(c_created or 0))
        daily_completed.append(int(c_completed or 0))
        daily_cancelled.append(int(c_cancelled or 0))

    # ==== Воронка: клик → запись → визит ====
    funnel = SimpleNamespace(
        clicks=int(clicks_count),
        created=int(created_count),
        completed=int(completed_count),
    )

    funnel_counts = {
        "clicks": funnel.clicks,
        "created": funnel.created,
        "completed": funnel.completed,
    }

    # ==== Аналитика по кнопкам ====
    # Получаем статистику нажатий на кнопки за период
    button_stats_q = (
        select(
            ButtonEvent.button_name,
            func.count(ButtonEvent.id).label("count"),
        )
        .where(ButtonEvent.created_at >= since)
        .group_by(ButtonEvent.button_name)
        .order_by(func.count(ButtonEvent.id).asc())  # От меньшего к большему (неиспользуемые сверху)
        .limit(15)
    )
    
    button_stats_res = await db.execute(button_stats_q)
    button_stats = [
        {
            "name": name,
            "count": int(count or 0),
        }
        for name, count in button_stats_res.all()
    ]
    
    # Получаем общее количество нажатий для расчёта процентов
    total_clicks_q = (
        select(func.count(ButtonEvent.id))
        .where(ButtonEvent.created_at >= since)
    )
    total_clicks_res = await db.execute(total_clicks_q)
    total_button_clicks = total_clicks_res.scalar() or 0

    return templates.TemplateResponse(
        "bookings_analytics.html",
        {
            "request": request,
            "stats": stats,
            # график по дням
            "daily_labels": daily_labels,
            "daily_created": daily_created,
            "daily_completed": daily_completed,
            "daily_cancelled": daily_cancelled,
            # воронка
            "funnel": funnel,
            "funnel_counts": funnel_counts,
            # аналитика кнопок
            "button_stats": button_stats,
            "total_button_clicks": total_button_clicks,
        },
    )

