import asyncio
import logging
import os
import re
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse

from aiogram import Bot, Dispatcher, F
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties
from aiogram.filters import CommandStart, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    CallbackQuery,
    Message,
    ReplyKeyboardRemove,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    FSInputFile,
    InputFile,
)
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from config import settings
from db import (
    init_db,
    AsyncSessionLocal,
    create_or_update_user,
    get_user_by_telegram_id,
    get_or_create_user_bonus,
    try_add_fixed_bonus,
    apply_referral_if_needed,
    reward_referral_after_visit,
    get_pending_referral_users,
    another_user_has_welcome_for_client,
    get_bonus_config,
    apply_promocode,
    sync_bonus_from_yclients,
    log_button_click,
    get_media_file_id,
    save_media_file_id,
    BookingEventType,
    User,
    ReviewBonusRequest,
)
from states import Registration, ReviewFlow, PromocodeFlow
from keyboards import (
    consent_keyboard,
    phone_request_keyboard,
    main_menu_keyboard,
    loyalty_menu_keyboard,
)
from yclients_client import YClientsClient
from booking_events import log_booking_event


logger = logging.getLogger(__name__)

# Глобальная ссылка на бота для уведомлений из scheduler
_bot_instance: Bot | None = None


# =========================
#   УТИЛЫ
# =========================

def build_booking_url(user_id: int | None = None) -> str:
    """
    Возвращает ссылку для записи.
    Если задан BOOKING_PROXY_URL — добавляет tg_id в query и использует прокси.
    """
    base = settings.BOOKING_PROXY_URL or settings.BOOKING_URL
    if settings.BOOKING_PROXY_URL and user_id is not None:
        parsed = urlparse(base)
        q = dict(parse_qsl(parsed.query))
        q["tg_id"] = str(user_id)
        base = urlunparse(parsed._replace(query=urlencode(q)))
    return base


# =========================
#   ХЕЛПЕР ДЛЯ ОТПРАВКИ МЕДИА
# =========================

async def send_media_with_fallback(
    message_or_callback: Message | CallbackQuery,
    media_path: str,
    photo_fallback_path: str = "",
    caption: str | None = None,
    reply_markup=None,
) -> None:
    """
    Отправляет MP4-клип (h264, без звука) с авто-плеем.
    Использует кэширование file_id для избежания flood control.

    Args:
        message_or_callback: Message или CallbackQuery объект
        media_path: Путь к MP4 файлу (например, "media/gifs/01_consent.mp4")
        photo_fallback_path: Путь к фото как резервный вариант (опционально)
        caption: Текст подписи
        reply_markup: Клавиатура
    """
    # Определяем объект Message
    if isinstance(message_or_callback, CallbackQuery):
        message = message_or_callback.message
    else:
        message = message_or_callback
    
    # Проверяем существование файла
    media_full_path = Path(media_path)
    if media_full_path.exists() and media_full_path.is_file():
        # Проверяем размер файла (Telegram автоматически воспроизводит только файлы < 5 МБ)
        file_size_mb = media_full_path.stat().st_size / (1024 * 1024)
        if file_size_mb > 5:
            logger.warning(f"⚠️  Media file {media_path} is {file_size_mb:.1f} MB (>5 MB). Telegram may not auto-play it.")
        
        # Проверяем кэш file_id
        async with AsyncSessionLocal() as session:
            cached_data = await get_media_file_id(session, media_path)

        cached_file_id = None
        cached_file_type = None
        if cached_data:
            # Поддерживаем старый формат (str) и новый формат (tuple[str, str])
            if isinstance(cached_data, (list, tuple)):
                if len(cached_data) >= 2:
                    cached_file_id, cached_file_type = cached_data[0], cached_data[1]
                elif len(cached_data) == 1:
                    cached_file_id = cached_data[0]
            elif isinstance(cached_data, str):
                cached_file_id = cached_data
                cached_file_type = "animation"
        
        try:
            if cached_file_id:
                await message.answer_animation(
                    animation=cached_file_id,
                    caption=caption,
                    reply_markup=reply_markup,
                )
                logger.info(f"Sent media from cache: {media_path} (file_id: {cached_file_id[:20]}...)")
                return
            
            # Отправляем файл заново (если кэша нет)
            # Для автоматического воспроизведения используем answer_animation
            sent_message = await message.answer_animation(
                animation=FSInputFile(media_path),
                caption=caption,
                reply_markup=reply_markup,
            )
            
            # Сохраняем file_id в кэш
            file_id = None
            file_type = "animation"
            
            # Проверяем animation (правильный тип для автоматического воспроизведения)
            if hasattr(sent_message, 'animation') and sent_message.animation:
                file_id = sent_message.animation.file_id
                file_type = "animation"
                logger.info(f"✅ Got animation file_id for {media_path}: {file_id[:20]}...")
            # Проверяем video (иногда Telegram может вернуть video)
            elif hasattr(sent_message, 'video') and sent_message.video:
                file_id = sent_message.video.file_id
                file_type = "animation"  # Сохраняем как animation для использования answer_animation
                logger.info(f"✅ Got media as video for {media_path}: {file_id[:20]}... (saving as animation)")
            # Проверяем document — не кэшируем, чтобы не зафиксировать неправильный тип
            elif hasattr(sent_message, 'document') and sent_message.document:
                doc = sent_message.document
                mime = getattr(doc, 'mime_type', None)
                logger.warning(f"⚠️  Media returned as document (mime_type: {mime}). NOT caching to avoid losing autoplay.")
            
            if file_id:
                async with AsyncSessionLocal() as session:
                    await save_media_file_id(session, media_path, file_id, file_type)
                logger.info(f"Sent media and cached file_id: {media_path} (file_id: {file_id[:20]}..., type: {file_type})")
            else:
                logger.warning(f"Sent media but no valid file_id in response: {media_path}")
            return
        except Exception as e:
            error_str = str(e).lower()
            # Если это временная ошибка (flood control, rate limit), отправляем только текст
            if "flood" in error_str or "rate limit" in error_str or "too many requests" in error_str:
                logger.warning(f"Temporary error sending animation {media_path}: {e}, sending text only")
                await message.answer(
                    text=caption or "",
                    reply_markup=reply_markup,
                )
                return
            logger.warning(f"Failed to send animation {media_path}: {e}, falling back to photo")
    
    # Fallback на фото
    if photo_fallback_path:
        photo_full_path = Path(photo_fallback_path)
        if photo_full_path.exists() and photo_full_path.is_file():
            # Проверяем кэш file_id для фото
            async with AsyncSessionLocal() as session:
                cached_data = await get_media_file_id(session, photo_fallback_path)

            cached_photo_id = None
            if cached_data:
                if isinstance(cached_data, (list, tuple)):
                    if len(cached_data) >= 1:
                        cached_photo_id = cached_data[0]
                elif isinstance(cached_data, str):
                    cached_photo_id = cached_data

            try:
                if cached_photo_id:
                    # Используем кэшированный file_id (для фото всегда answer_photo)
                    await message.answer_photo(
                        photo=cached_photo_id,
                        caption=caption,
                        reply_markup=reply_markup,
                    )
                    logger.info(f"Sent photo from cache: {photo_fallback_path} (file_id: {cached_photo_id[:20]}...)")
                    return
                else:
                    # Отправляем файл и сохраняем file_id
                    sent_message = await message.answer_photo(
                        photo=FSInputFile(photo_fallback_path),
                        caption=caption,
                        reply_markup=reply_markup,
                    )
                    
                    # Сохраняем file_id в кэш
                    if sent_message.photo:
                        file_id = sent_message.photo[-1].file_id  # Берем самое большое фото
                        async with AsyncSessionLocal() as session:
                            await save_media_file_id(session, photo_fallback_path, file_id, "photo")
                        logger.info(f"Sent photo and cached file_id: {photo_fallback_path} (file_id: {file_id[:20]}...)")
                    else:
                        logger.warning(f"Sent photo but no file_id in response: {photo_fallback_path}")
                    return
            except Exception as e:
                logger.warning(f"Failed to send photo fallback {photo_fallback_path}: {e}")
    
    # Если и фото нет, отправляем только текст
    await message.answer(
        text=caption or "",
        reply_markup=reply_markup,
    )
    logger.warning(f"Media file not found ({media_path}) and no fallback photo, sent text only")


# =========================
#   АВТОМАТИЧЕСКАЯ ПРОВЕРКА РЕФЕРАЛЬНЫХ БОНУСОВ
# =========================

# Минимальное время после визита для начисления бонуса (30 минут)
REFERRAL_CHECK_DELAY_MINUTES = 30


async def check_referral_bonuses_job():
    """
    Периодическая задача: проверяет всех пользователей, ожидающих реферальный бонус.
    Если визит состоялся более 30 минут назад — начисляет бонус.
    """
    global _bot_instance
    
    logger.info("[REFERRAL_JOB] Starting referral bonus check...")
    
    yclients_client = YClientsClient()
    
    async with AsyncSessionLocal() as session:
        pending_users = await get_pending_referral_users(session)
        
        if not pending_users:
            logger.info("[REFERRAL_JOB] No pending referral users found")
            return
        
        logger.info("[REFERRAL_JOB] Found %d pending referral users", len(pending_users))
        
        config = await get_bonus_config(session)
        referral_amount = config.referral_amount
        
        for user, bonus in pending_users:
            try:
                # Проверяем, есть ли завершённый визит
                visit_dt = await yclients_client.has_completed_visit(user.yclients_client_id)
                
                if not visit_dt:
                    logger.debug(
                        "[REFERRAL_JOB] user_id=%s: no completed visit yet",
                        user.id,
                    )
                    continue
                
                # Проверяем, прошло ли 30 минут после визита
                now = datetime.utcnow()
                time_since_visit = now - visit_dt
                
                if time_since_visit < timedelta(minutes=REFERRAL_CHECK_DELAY_MINUTES):
                    logger.debug(
                        "[REFERRAL_JOB] user_id=%s: visit too recent (%s ago), waiting...",
                        user.id,
                        time_since_visit,
                    )
                    continue
                
                # Начисляем реферальный бонус
                bonus_after, inviter_bonus, granted = await reward_referral_after_visit(
                    session=session,
                    user=user,
                    amount=referral_amount,
                    visit_dt=visit_dt,
                )
                
                if granted and _bot_instance and inviter_bonus:
                    # Уведомляем инвайтера
                    inviter_user = await session.get(User, inviter_bonus.user_id)
                    if inviter_user and inviter_user.telegram_id:
                        friend_name = user.full_name or user.username or "ваш друг"
                        try:
                            await _bot_instance.send_message(
                                inviter_user.telegram_id,
                                f"🎉 Ваш друг <b>{friend_name}</b> посетил салон!\n\n"
                                f"Вам начислен реферальный бонус: <b>{referral_amount}₽</b>.\n"
                                f"Текущий баланс: <b>{inviter_bonus.balance}₽</b>.",
                            )
                            logger.info(
                                "[REFERRAL_JOB] Notified inviter: user_id=%s tg_id=%s amount=%s",
                                inviter_user.id,
                                inviter_user.telegram_id,
                                referral_amount,
                            )
                        except Exception as e:
                            logger.warning(
                                "[REFERRAL_JOB] Failed to notify inviter user_id=%s: %s",
                                inviter_user.id,
                                e,
                            )
                    
                    # Уведомляем приглашённого
                    if user.telegram_id:
                        try:
                            await _bot_instance.send_message(
                                user.telegram_id,
                                f"🎉 Ваш визит зафиксирован!\n\n"
                                f"Реферальный бонус <b>{referral_amount}₽</b> начислен другу, "
                                f"который поделился с вами ссылкой.",
                            )
                        except Exception as e:
                            logger.warning(
                                "[REFERRAL_JOB] Failed to notify invited user_id=%s: %s",
                                user.id,
                                e,
                            )
                
                if granted:
                    logger.info(
                        "[REFERRAL_JOB] Bonus granted: invited_user_id=%s inviter_user_id=%s amount=%s",
                        user.id,
                        inviter_bonus.user_id if inviter_bonus else None,
                        referral_amount,
                    )
                    
            except Exception as e:
                logger.exception(
                    "[REFERRAL_JOB] Error processing user_id=%s: %s",
                    user.id,
                    e,
                )
    
    logger.info("[REFERRAL_JOB] Referral bonus check completed")


async def _log_callback_button(callback: CallbackQuery, button_label: str) -> None:
    """Вспомогательная функция для логирования inline-кнопок."""
    try:
        async with AsyncSessionLocal() as session:
            user = await get_user_by_telegram_id(session, callback.from_user.id)
            await log_button_click(
                session=session,
                user_id=user.id if user else None,
                button_name=f"[callback] {button_label}",
            )
    except Exception:
        logger.exception("Failed to log callback button: tg_id=%s", callback.from_user.id)


def normalize_phone(phone: str) -> str:
    """
    Простейшая нормализация под российский формат для YCLIENTS:
    - оставляем только цифры
    - приводим к виду 7XXXXXXXXXX
    """
    digits = re.sub(r"\D", "", phone)

    if digits.startswith("8") and len(digits) == 11:
        digits = "7" + digits[1:]
    elif digits.startswith("7") and len(digits) == 11:
        pass
    elif digits.startswith("9") and len(digits) == 10:
        digits = "7" + digits
    # если формат какой-то другой — оставляем как есть, но можно расширить

    return digits


def format_record_for_button(record: dict) -> str:
    """
    Форматирование записи для подписи на кнопке:
    - Дата и время
    - Имя мастера (если есть)
    """
    dt_raw = record.get("datetime") or record.get("date") or ""
    staff = record.get("staff") or {}
    if isinstance(staff, dict):
        staff_name = staff.get("name") or ""
    else:
        staff_name = ""

    dt_short = ""
    if dt_raw:
        try:
            # YCLIENTS обычно отдает ISO8601
            dt = datetime.fromisoformat(dt_raw.replace("Z", "+00:00"))
            dt_short = dt.strftime("%d.%m %H:%M")
        except Exception:
            dt_short = dt_raw

    parts = []
    if dt_short:
        parts.append(dt_short)
    if staff_name:
        parts.append(staff_name)

    if not parts:
        return f"Запись #{record.get('id')}"
    return " – ".join(parts)


def extract_referral_code_from_start(text: str | None) -> str | None:
    """
    Парсим /start ref_XXXXXX и возвращаем код, если он есть.
    """
    if not text:
        return None

    parts = text.split(maxsplit=1)
    if len(parts) < 2:
        return None

    payload = parts[1].strip()
    if payload.startswith("ref_") and len(payload) > 4:
        return payload[4:]
    return None


async def handle_start(message: Message, state: FSMContext) -> None:
    """
    /start:
    - Сохраняем возможный реферальный код в FSM
    - Показываем политику и юр.пакет + кнопку "Согласен".
    """
    await state.clear()

    ref_code = extract_referral_code_from_start(message.text)
    if ref_code:
        await state.update_data(referral_code=ref_code)
        logger.info(
            "Start with referral: tg_id=%s ref_code=%s",
            message.from_user.id,
            ref_code,
        )
    else:
        logger.info("Start without referral: tg_id=%s", message.from_user.id)

    # Сообщение с согласием
    policy_text = (
        "👋 Добро пожаловать в <b>Demo Lounge</b>\n\n"
        "Перед тем как продолжить, важно подтвердить согласие с документами:\n"
        f"• <a href=\"{settings.PRIVACY_POLICY_URL}\">Политика конфиденциальности</a>\n"
        f"• <a href=\"{settings.LEGAL_DOCS_URL}\">Юридический пакет</a>\n\n"
        "Нажимая кнопку <b>«Согласен»</b> ниже, вы подтверждаете, что ознакомились с ними."
    )

    await send_media_with_fallback(
        message,
        media_path="media/gifs/01_consent.mp4",
        photo_fallback_path="",  # Убираем fallback для WebM
        caption=policy_text,
        reply_markup=consent_keyboard(),
    )

    await state.set_state(Registration.awaiting_consent)


async def on_consent_accepted(callback: CallbackQuery, state: FSMContext) -> None:
    """
    Пользователь нажал "Согласен" — сохраняем согласие и запрашиваем телефон.
    """
    await callback.answer()

    async with AsyncSessionLocal() as session:
        user = await create_or_update_user(
            session=session,
            telegram_id=callback.from_user.id,
            username=callback.from_user.username,
            full_name=callback.from_user.full_name,
            agreed_privacy=True,
        )

    logger.info(
        "Consent accepted: tg_id=%s user_id=%s",
        callback.from_user.id,
        user.id,
    )

    text = (
        "Отлично, двигаемся дальше 💼\n\n"
        "Чтобы персонализировать сервис и связать вас с клиентской базой, "
        "нужно подтвердить номер телефона.\n\n"
        "📱 Вы можете:\n"
        "• отправить свой номер через кнопку «Взять номер из Telegram»\n"
        "• или ввести номер вручную в формате <b>+7 999 123-45-67</b>"
    )

    await send_media_with_fallback(
        callback,
            media_path="media/gifs/02_phone_request.mp4",
        photo_fallback_path="",  # Убираем fallback для WebM
        caption=text,
        reply_markup=phone_request_keyboard(),
    )

    await state.set_state(Registration.awaiting_phone)


async def process_contact(message: Message, state: FSMContext) -> None:
    """
    Обработка телефона, полученного из контакта Telegram.

    ВАЖНО:
    - НЕЛЬЗЯ перепрыгивать шаг согласия с документами.
    - Разрешаем контакт:
      * когда мы явно ждём телефон (awaiting_phone), ИЛИ
      * когда состояние потерялось (None), но в БД уже есть agreed_privacy=True и phone=NULL
        — это кейс восстановления после рестарта.
    """
    if not message.contact or not message.contact.phone_number:
        await message.answer("Не удалось получить номер телефона. Попробуйте ввести его вручную.")
        return

    current_state = await state.get_state()

    # 1) Если мы на шаге согласия — контакты игнорируем как номер
    if current_state is not None and current_state.endswith("awaiting_consent"):
        await message.answer(
            "Чтобы продолжить, пожалуйста, сначала нажмите кнопку <b>«Согласен»</b> "
            "под предыдущим сообщением.\n\n"
            "После этого появится шаг с вводом номера телефона."
        )
        return

    # 2) Если явно ждём телефон — всё ок, принимаем контакт
    if current_state is not None and current_state.endswith("awaiting_phone"):
        phone_raw = message.contact.phone_number
        logger.info(
            "Phone contact received (FSM awaiting_phone): tg_id=%s phone_raw=%s",
            message.from_user.id,
            phone_raw,
        )
        await process_phone_common(message, state, phone_raw)
        return

    # 3) Состояние потеряно (None) — возможно, бот перезапускался между согласием и телефоном.
    if current_state is None:
        async with AsyncSessionLocal() as session:
            user = await get_user_by_telegram_id(session, message.from_user.id)

        # Если юзер есть, он согласился с политикой, но телефон ещё не записан —
        # позволяем завершить регистрацию через контакт.
        if user and user.agreed_privacy and not user.phone:
            phone_raw = message.contact.phone_number
            logger.info(
                "Phone contact received after restart: tg_id=%s phone_raw=%s user_id=%s",
                message.from_user.id,
                phone_raw,
                user.id,
            )
            await process_phone_common(message, state, phone_raw)
            return

        # Иначе — просим пройти путь заново корректно.
        await message.answer(
            "Чтобы продолжить, пожалуйста, начните с команды /start и подтвердите согласие "
            "с документами. После этого бот попросит номер телефона."
        )
        return

    # На всякий случай fallback: если вдруг появятся другие состояния
    await message.answer(
        "Сейчас я не ожидаю номер телефона.\n\n"
        "Если вы хотите начать регистрацию заново — используйте команду /start."
    )


async def process_phone_text(message: Message, state: FSMContext) -> None:
    """
    Обработка телефона, введённого вручную.

    Работает:
    - в состоянии awaiting_phone (обычный сценарий);
    - при state=None, если в БД есть agreed_privacy=True и phone=NULL (восстановление после рестарта);
    - НЕ позволяет перепрыгнуть согласие с документами.
    """
    text = (message.text or "").strip()
    current_state = await state.get_state()

    # Отмена
    if text.lower() == "отмена":
        if current_state is not None and current_state.endswith("awaiting_phone"):
            await message.answer(
                "Действие отменено. Чтобы начать заново, используйте команду /start.",
                reply_markup=ReplyKeyboardRemove(),
            )
            await state.clear()
        else:
            await message.answer(
                "Сейчас регистрация неактивна.\n\n"
                "Если вы хотите пройти регистрацию, начните с команды /start.",
            )
        return

    # НЕЛЬЗЯ перепрыгивать шаг согласия
    if current_state is not None and current_state.endswith("awaiting_consent"):
        await message.answer(
            "Чтобы продолжить, пожалуйста, сначала нажмите кнопку <b>«Согласен»</b> "
            "под предыдущим сообщением.\n\n"
            "После этого появится шаг с вводом номера телефона."
        )
        return

    # Обычный сценарий: явно ждём телефон
    if current_state is not None and current_state.endswith("awaiting_phone"):
        logger.info(
            "Phone text received (FSM awaiting_phone): tg_id=%s text=%s",
            message.from_user.id,
            text,
        )
        phone_raw = text
        await process_phone_common(message, state, phone_raw)
        return

    # Состояние потеряно (None) — возможно, бот перезапускался между согласием и телефоном.
    if current_state is None:
        async with AsyncSessionLocal() as session:
            user = await get_user_by_telegram_id(session, message.from_user.id)

        if user and user.agreed_privacy and not user.phone:
            logger.info(
                "Phone text received after restart: tg_id=%s text=%s user_id=%s",
                message.from_user.id,
                text,
                user.id,
            )
            phone_raw = text
            await process_phone_common(message, state, phone_raw)
            return

        # Иначе — это просто текст вне контекста регистрации
        await message.answer(
            "Сейчас я не ожидаю номер телефона.\n\n"
            "Чтобы пройти регистрацию, начните с команды /start.",
        )
        return

    # Любые другие состояния (если вдруг появятся в будущем)
    await message.answer(
        "Сейчас я не ожидаю номер телефона.\n\n"
        "Если вы хотите начать регистрацию заново — используйте команду /start.",
    )


async def process_phone_common(message: Message, state: FSMContext, phone_raw: str) -> None:
    """
    Общая логика:
    - нормализуем телефон
    - безопасно обращаемся к YCLIENTS (различаем "нет клиента" и "ошибка API")
    - если не нашли — создаём там клиента
    - сохраняем в БД флаг is_new_client (фиксируем только факт "был ли в YCLIENTS до нас")
    - привязываем реферальный код (без начисления бонуса)
    - при входе по реферальной ссылке один раз объясняем механику
    - показываем главное меню
    """
    phone = normalize_phone(phone_raw)

    if len(phone) < 10:
        await message.answer("Похоже, номер введён некорректно. Попробуйте ещё раз.")
        logger.info(
            "Phone validation failed: tg_id=%s phone_raw=%s normalized=%s",
            message.from_user.id,
            phone_raw,
            phone,
        )
        return

    logger.info(
        "Phone processing start: tg_id=%s phone_raw=%s normalized=%s",
        message.from_user.id,
        phone_raw,
        phone,
    )

    yclients_client = YClientsClient()

    # 1) Пробуем найти клиента в YCLIENTS
    yclients_data, lookup_error = await yclients_client.find_client_by_phone(phone)

    if lookup_error:
        logger.warning(
            "YCLIENTS lookup error: tg_id=%s phone=%s",
            message.from_user.id,
            phone,
        )
        await message.answer(
            "Сейчас не удаётся обратиться к клиентской базе салона.\n\n"
            "Пожалуйста, попробуйте чуть позже или свяжитесь с администратором салона."
        )
        return

    # Был ли номер в YCLIENTS до нашего запроса
    client_was_missing = yclients_data is None

    # 2) Если клиента не нашли — создаём его в YCLIENTS
    if client_was_missing:
        full_name = message.from_user.full_name or ""
        created, create_error = await yclients_client.create_client(phone=phone, name=full_name)

        if create_error or not created:
            logger.warning(
                "YCLIENTS create_client error: tg_id=%s phone=%s full_name=%s",
                message.from_user.id,
                phone,
                full_name,
            )
            await message.answer(
                "Сейчас не удаётся сохранить ваши данные в клиентской базе салона.\n\n"
                "Попробуйте чуть позже или свяжитесь с администратором салона."
            )
            return

        yclients_data = created

    # На этом этапе:
    # - если client_was_missing == True, то мы только что создали клиента в YCLIENTS
    # - если False, то он уже был там до бота
    is_new_client = client_was_missing
    yclients_client_id = yclients_data.get("id") if yclients_data else None

    logger.info(
        "YCLIENTS link result: tg_id=%s yclients_client_id=%s is_new_client=%s client_was_missing=%s",
        message.from_user.id,
        yclients_client_id,
        is_new_client,
        client_was_missing,
    )

    state_data = await state.get_data()
    referral_code = state_data.get("referral_code")

    async with AsyncSessionLocal() as session:
        # Проверяем, существовал ли пользователь ДО регистрации
        existing_user = await get_user_by_telegram_id(session, message.from_user.id)
        is_first_registration = existing_user is None
        
        user = await create_or_update_user(
            session=session,
            telegram_id=message.from_user.id,
            username=message.from_user.username,
            full_name=message.from_user.full_name,
            phone=phone,
            yclients_client_id=yclients_client_id,
            is_new_client=is_new_client,
        )

        # Привязываем реферальный код ТОЛЬКО при первой регистрации И для новых клиентов YClients
        if is_first_registration:
            await apply_referral_if_needed(
                session=session,
                user=user,
                referral_code=referral_code,
            )
        elif referral_code:
            logger.info(
                "Referral code ignored for existing user: tg_id=%s referral_code=%s",
                message.from_user.id,
                referral_code,
            )

        # Обеспечиваем наличие записи в таблице бонусов (для реферального кода и баланса)
        bonus = await get_or_create_user_bonus(session, user)

        # Если пришёл по реферальной ссылке, но он старый клиент YClients — уведомляем
        if referral_code and not is_new_client and is_first_registration:
            await message.answer(
                "⚠️ Вы перешли по реферальной ссылке, но уже являетесь клиентом "
                "Ashstyle Barber Lounge.\n\n"
                "Реферальная программа доступна только для новых клиентов, "
                "ранее не посещавших салон."
            )
            logger.info(
                "Referral link rejected for old YClients client: tg_id=%s referral_code=%s",
                message.from_user.id,
                referral_code,
            )

        # Если пользователь пришёл по реферальной ссылке (и он новый клиент) — один раз объясняем механику
        if bonus.referred_by_code and not bonus.referred_registration_notified:
            await message.answer(
                "🤝 Вы авторизовались по реферальной ссылке!\n\n"
                "Чтобы ваш друг получил реферальный бонус, достаточно записаться на любую услугу "
                "Ashstyle Barber Lounge и посетить её.\n\n"
                "Бонус будет начислен автоматически примерно через 30 минут после вашего визита.\n\n"
                "Для вас также доступен Welcome bonus, если вы новый клиент."
            )
            bonus.referred_registration_notified = True
            await session.commit()
            logger.info(
                "Referral registration notice shown: invited_user_id=%s referred_by_code=%s",
                user.id,
                bonus.referred_by_code,
            )

    # Тексты для статуса
    if is_new_client:
        status_text = (
            "🎉 Добро пожаловать в бонусную систему <b>Demo Lounge</b>!\n\n"
            "Мы создали для вас профиль клиента. Теперь вы сможете:\n"
            "• участвовать в бонусной программе\n"
            "• получать персональные предложения\n"
            "• удобнее управлять своими записями\n\n"
            "<b>Главное меню — ниже.</b>"
        )

        await send_media_with_fallback(
            message,
            media_path="media/gifs/03_welcome_existing.mp4",
            photo_fallback_path="",  # Убираем fallback для WebM
            caption=status_text,
            reply_markup=main_menu_keyboard(),
        )
    else:
        status_text = (
            "🤝 Рады снова видеть вас в <b>Demo Lounge</b>!\n\n"
            "Мы нашли ваш профиль в клиентской базе и связали его с этим Telegram-аккаунтом.\n\n"
            "Главное меню — ниже."
        )

        await send_media_with_fallback(
            message,
            media_path="media/gifs/03_welcome_existing.mp4",
            photo_fallback_path="",  # Убираем fallback для WebM
            caption=status_text,
            reply_markup=main_menu_keyboard(),
        )

    await state.clear()

    logger.info(
        "Phone processing finished: tg_id=%s yclients_client_id=%s is_new_client=%s",
        message.from_user.id,
        yclients_client_id,
        is_new_client,
    )


async def handle_text_awaiting_consent(message: Message, state: FSMContext) -> None:
    """
    Любое текстовое сообщение, пока ждём согласия.
    Вместо молчания — мягкий пинок: "нажмите Согласен".
    """
    await message.answer(
        "Чтобы продолжить, пожалуйста, нажмите кнопку <b>«Согласен»</b> "
        "под предыдущим сообщением.\n\n"
        "Без согласия с документами мы не можем перейти к следующему шагу 🤝"
    )


async def start_cancel_flow(message: Message, telegram_id: int) -> None:
    """
    Общий сценарий показа списка записей для отмены (по кнопке или по callback).
    """
    async with AsyncSessionLocal() as session:
        user = await get_user_by_telegram_id(session, telegram_id)

        if user is None or user.yclients_client_id is None:
            await message.answer(
                "Не удалось найти ваш профиль в базе клиентов YCLIENTS.\n\n"
                "Пожалуйста, начните заново с команды /start, чтобы привязать номер телефона.",
                reply_markup=main_menu_keyboard(),
            )
            return

        yclients_client = YClientsClient()
        records = await yclients_client.get_upcoming_records(
            user.yclients_client_id,
            days_ahead=30,
        )

    if not records:
        await message.answer(
            "У вас нет активных записей в ближайшие 30 дней.",
            reply_markup=main_menu_keyboard(),
        )
        return

    buttons = []
    for record in records:
        record_id = record.get("id")
        if not record_id:
            continue
        text_btn = format_record_for_button(record)
        buttons.append(
            [
                InlineKeyboardButton(
                    text=text_btn,
                    callback_data=f"cancel_record:{record_id}",
                )
            ]
        )

    # кнопка отмены выбора
    buttons.append(
        [InlineKeyboardButton(text="🔙 Оставить всё как есть", callback_data="cancel_cancel")]
    )

    kb = InlineKeyboardMarkup(inline_keyboard=buttons)

    await message.answer(
        "Выберите запись, которую хотите отменить:",
        reply_markup=kb,
    )


async def handle_main_menu(message: Message, state: FSMContext) -> None:
    """
    Обработка кнопок главного меню и меню бонусной программы.

    ВАЖНО: сюда мы попадаем только когда пользователь уже прошёл регистрацию
    (нет активного FSM-состояния).
    """
    # Дополнительная защита (на случай, если фильтр когда-нибудь изменится)
    current_state = await state.get_state()
    if current_state is not None:
        if current_state.endswith("awaiting_consent"):
            await message.answer(
                "Сначала, пожалуйста, подтвердите согласие с документами, "
                "нажав кнопку «Согласен» под предыдущим сообщением."
            )
        elif current_state.endswith("awaiting_phone"):
            await message.answer(
                "Пожалуйста, отправьте номер телефона или используйте кнопку "
                "«📲 Взять номер из Telegram» под предыдущим сообщением."
            )
        return

    text = message.text
    logger.info("Main menu text: tg_id=%s text=%s", message.from_user.id, text)

    # Логируем нажатие на кнопку для аналитики
    if text:
        try:
            async with AsyncSessionLocal() as session:
                user = await get_user_by_telegram_id(session, message.from_user.id)
                await log_button_click(
                    session=session,
                    user_id=user.id if user else None,
                    button_name=text,
                )
        except Exception:
            logger.exception("Failed to log button click: tg_id=%s", message.from_user.id)

    if text == "💈 Запись":
        kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text="💈 Записаться онлайн",
                        url=build_booking_url(message.from_user.id),
                    )
                ],
                [
                    InlineKeyboardButton(
                        text="🗓 Отмена записи",
                        callback_data="cancel_start",
                    )
                ],
            ]
        )

        await send_media_with_fallback(
            message,
            media_path="media/gifs/04_booking.mp4",
            photo_fallback_path="",  # Убираем fallback для WebM
            caption=(
                "Онлайн-запись в салон доступна 24/7.\n\n"
                "Через удобную форму вы можете:\n"
                "• выбрать мастера\n"
                "• указать услугу\n"
                "• подобрать удобное время\n\n"
                "Нажмите кнопку ниже, чтобы продолжить 👇"
            ),
            reply_markup=kb,
        )

    elif text == "🗓 Отмена записи":
        await start_cancel_flow(message, message.from_user.id)

    elif text == "🎁 Бонусная программа":
        async with AsyncSessionLocal() as session:
            user = await get_user_by_telegram_id(session, message.from_user.id)
            if user:
                # Синхронизируем баланс ИЗ YClients
                yclients_balance = await sync_bonus_from_yclients(session, user)
                if yclients_balance is not None:
                    balance = yclients_balance
                else:
                    bonus = await get_or_create_user_bonus(session, user)
                    balance = bonus.balance
                
                bonus = await get_or_create_user_bonus(session, user)
                
                # Проверяем, был ли первый визит и не отправляли ли мы уведомление
                if user.yclients_client_id and not bonus.first_visit_review_notified:
                    yclients_client = YClientsClient()
                    visit_dt = await yclients_client.has_completed_visit(user.yclients_client_id)
                    
                    if visit_dt:
                        # Отправляем уведомление о возможности оставить отзыв
                        await message.answer(
                            "🎉 <b>Спасибо за визит!</b>\n\n"
                            "Мы видим, что вы недавно посетили наш салон. "
                            "Будем очень благодарны, если оставите отзыв на Яндекс.Картах или 2ГИС!\n\n"
                            "За каждый подтверждённый отзыв вы получите бонусные рубли. "
                            "Перейдите в раздел «⭐ Бонус за отзыв» ниже.",
                        )
                        bonus.first_visit_review_notified = True
                        await session.commit()
            else:
                balance = 0

        await send_media_with_fallback(
            message,
            media_path="media/gifs/06_bonus_program.mp4",
            photo_fallback_path="",  # Нет fallback фото
            caption=(
                "🎁 <b>Бонусная программа Demo Lounge</b>\n\n"
                f"Текущий бонусный баланс: <b>{balance}₽</b>\n\n"
                "Доступны следующие активности:\n"
                "• 🎉 Welcome bonus — за активацию бонусной программы (только для новых клиентов)\n"
                "• 📢 Бонус за подписку на канал — единоразово\n"
                "• ⭐ Бонус за отзыв — отдельно за Яндекс и 2ГИС\n"
                "• 🤝 Реферальная программа — бонусы за приглашённых друзей\n\n"
                "Реферальный бонус начисляется автоматически после визита приглашённого друга.\n\n"
                "Выберите интересующий пункт в меню ниже."
            ),
            reply_markup=loyalty_menu_keyboard(),
        )

    elif text == "💰 Баланс":
        async with AsyncSessionLocal() as session:
            user = await get_user_by_telegram_id(session, message.from_user.id)
            if user:
                # Синхронизируем баланс ИЗ YClients (если клиент потратил бонусы в салоне)
                yclients_balance = await sync_bonus_from_yclients(session, user)
                if yclients_balance is not None:
                    balance = yclients_balance
                else:
                    bonus = await get_or_create_user_bonus(session, user)
                    balance = bonus.balance
            else:
                balance = 0

        await send_media_with_fallback(
            message,
            media_path="media/gifs/11_balance.mp4",
            photo_fallback_path="",  # Нет fallback фото
            caption=(
                "💰 <b>Ваш бонусный баланс</b>\n\n"
                f"На вашем счёте сейчас: <b>{balance}₽</b> бонусов.\n\n"
                "Бонусы можно использовать как скидку при оплате услуг в салоне (до 30% от чека)."
            ),
            reply_markup=main_menu_keyboard(),
        )

    elif text == "🆘 Помощь":
        kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text="🗣 Написать в поддержку",
                        url=settings.SUPPORT_CHAT_URL,
                    )
                ],
                [
                    InlineKeyboardButton(
                        text="👥 Сообщество проекта",
                        url=settings.COMMUNITY_URL,
                    )
                ],
            ]
        )

        await send_media_with_fallback(
            message,
            media_path="media/gifs/12_help.mp4",
            photo_fallback_path="",  # Нет fallback фото
            caption=(
                "🆘 <b>Помощь</b>\n\n"
                "Если у вас возникли вопросы по записи, бонусам или работе бота —\n"
                "вы можете написать в чат поддержки или зайти в сообщество проекта."
            ),
            reply_markup=kb,
        )

    elif text == "🎉 Welcome bonus":
        async with AsyncSessionLocal() as session:
            user = await get_user_by_telegram_id(session, message.from_user.id)
            if user is None:
                await message.answer(
                    "Не удалось найти ваш профиль. Пожалуйста, начните с команды /start.",
                    reply_markup=main_menu_keyboard(),
                )
                return

            config = await get_bonus_config(session)
            
            # Проверяем, включён ли Welcome bonus
            if not config.welcome_enabled:
                await message.answer(
                    "🚫 Welcome bonus временно отключён.\n\n"
                    "Следите за новостями — скоро он снова станет доступен!",
                    reply_markup=loyalty_menu_keyboard(),
                )
                return
            
            welcome_amount = config.welcome_amount
            referral_amount = config.referral_amount

            # Критично: Welcome только для новых клиентов (is_new_client == True)
            if not user.is_new_client:
                # Обычный клиент — не даём Welcome, а предлагаем реферальную механику
                await send_media_with_fallback(
                    message,
                    media_path="media/gifs/05_welcome_unavailable.mp4",
                    photo_fallback_path="",  # Убираем fallback для WebM
                    caption=(
                        "Welcome bonus доступен только новым клиентам, ранее не посещавшим салон.\n\n"
                        "Поделитесь реферальной ссылкой с другом — за приглашение вы получите "
                        f"<b>{referral_amount}₽</b> бонусных рублей, если друг воспользуется любой услугой "
                        "в салоне.\n\n"
                        "Найти свою ссылку можно в разделе «🤝 Реферальная программа»."
                    ),
                    reply_markup=InlineKeyboardMarkup(
                        inline_keyboard=[
                            [
                                InlineKeyboardButton(
                                    text="🤝 Реферальная программа",
                                    callback_data="open_ref_program",
                                )
                            ]
                        ]
                    ),
                )
                return

            # Доп. защита: один welcome на одного клиента YCLIENTS
            if user.yclients_client_id is not None:
                another_has = await another_user_has_welcome_for_client(
                    session=session,
                    yclients_client_id=user.yclients_client_id,
                    current_user_id=user.id,
                )
                if another_has:
                    await message.answer(
                        "По этому номеру телефона welcome bonus уже был активирован ранее "
                        "из другого аккаунта.\n\n"
                        "Welcome bonus выдаётся один раз на реального клиента, "
                        "но вы можете заработать бонусы через реферальную программу ✂️",
                        reply_markup=loyalty_menu_keyboard(),
                    )
                    return

            # Новый клиент — стандартная логика начисления welcome-бонуса
            bonus, granted = await try_add_fixed_bonus(
                session=session,
                user=user,
                flag_field="welcome_given",
                amount=welcome_amount,
            )

        if granted:
            await send_media_with_fallback(
                message,
                media_path="media/gifs/07_welcome_bonus_granted.mp4",
                photo_fallback_path="",  # Нет fallback фото
                caption=(
                    f"🎉 Welcome bonus начислен: <b>{welcome_amount}₽</b>.\n"
                    f"Текущий баланс: <b>{bonus.balance}₽</b>."
                ),
                reply_markup=loyalty_menu_keyboard(),
            )
        else:
            await message.answer(
                "Вы уже получали Welcome bonus ранее 🙂\n"
                f"Текущий баланс: <b>{bonus.balance}₽</b>.",
                reply_markup=loyalty_menu_keyboard(),
            )

    elif text == "📢 Бонус за подписку на канал":
        async with AsyncSessionLocal() as session:
            config = await get_bonus_config(session)
            
            # Проверяем, включён ли бонус за подписку
            if not config.channel_enabled:
                await message.answer(
                    "🚫 Бонус за подписку на канал временно отключён.\n\n"
                    "Следите за новостями — скоро он снова станет доступен!",
                    reply_markup=loyalty_menu_keyboard(),
                )
                return
            
            channel_amount = config.channel_amount

        kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text="📢 Открыть канал",
                        url=settings.COMMUNITY_URL,
                    )
                ],
                [
                    InlineKeyboardButton(
                        text="✅ Я подписался",
                        callback_data="bonus_channel_confirm",
                    )
                ],
            ]
        )

        await send_media_with_fallback(
            message,
            media_path="media/gifs/08_channel_bonus.mp4",
            photo_fallback_path="",  # Нет fallback фото
            caption=(
                "📢 <b>Бонус за подписку на канал</b>\n\n"
                "1) Перейдите в официальный канал проекта по кнопке ниже.\n"
                "2) Подпишитесь на канал.\n"
                "3) Вернитесь в этот чат и нажмите «✅ Я подписался».\n\n"
                f"За подписку будет начислено <b>{channel_amount}₽</b> один раз на клиента."
            ),
            reply_markup=kb,
        )

    elif text == "⭐ Бонус за отзыв":
        # Проверяем, включён ли бонус за отзыв
        async with AsyncSessionLocal() as session:
            config = await get_bonus_config(session)
            if not config.review_enabled:
                await message.answer(
                    "🚫 Бонус за отзыв временно отключён.\n\n"
                    "Следите за новостями — скоро он снова станет доступен!",
                    reply_markup=loyalty_menu_keyboard(),
                )
                return

        # Показываем выбор платформы: Яндекс или 2ГИС
        kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text="🗺 Яндекс.Карты",
                        callback_data="review_platform_yandex",
                    )
                ],
                [
                    InlineKeyboardButton(
                        text="🌐 2ГИС",
                        callback_data="review_platform_2gis",
                    )
                ],
            ]
        )

        await send_media_with_fallback(
            message,
            media_path="media/gifs/09_review_bonus.mp4",
            photo_fallback_path="",  # Нет fallback фото
            caption=(
                "⭐ <b>Бонус за отзыв</b>\n\n"
                "Вы можете получить бонусы за отзыв на одной из платформ:\n"
                "• Яндекс.Карты\n"
                "• 2ГИС\n\n"
                "За каждый подтверждённый отзыв начисляются бонусные рубли.\n"
                "Выберите платформу, на которой оставили отзыв:"
            ),
            reply_markup=kb,
        )

    elif text == "🤝 Реферальная программа":
        async with AsyncSessionLocal() as session:
            user = await get_user_by_telegram_id(session, message.from_user.id)
            if user is None:
                await message.answer(
                    "Не удалось найти ваш профиль. Пожалуйста, начните с команды /start.",
                    reply_markup=main_menu_keyboard(),
                )
                return

            bonus = await get_or_create_user_bonus(session, user)
            balance = bonus.balance

            config = await get_bonus_config(session)
            
            # Проверяем, включена ли реферальная программа
            if not config.referral_enabled:
                await message.answer(
                    "🚫 Реферальная программа временно отключена.\n\n"
                    f"Ваш текущий баланс: <b>{balance}₽</b>.\n\n"
                    "Следите за новостями — скоро она снова станет доступна!",
                    reply_markup=loyalty_menu_keyboard(),
                )
                return
            
            referral_amount = config.referral_amount

        me = await message.bot.get_me()
        deep_link = f"https://t.me/{me.username}?start=ref_{bonus.referral_code}"

        kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text="🔗 Получить реферальную ссылку",
                        callback_data="get_ref_link",
                    )
                ]
            ]
        )

        await send_media_with_fallback(
            message,
            media_path="media/gifs/10_referral_program.mp4",
            photo_fallback_path="",  # Нет fallback фото
            caption=(
                "🤝 <b>Реферальная программа</b>\n\n"
                f"Поделитесь реферальной ссылкой с другом — за приглашение вы получите "
                f"<b>{referral_amount}₽</b> бонусных рублей, если друг воспользуется любой услугой "
                "в Ashstyle Barber Lounge.\n\n"
                "Ниже вы можете запросить свою персональную ссылку.\n\n"
                f"Текущий баланс: <b>{balance}₽</b>."
            ),
            reply_markup=kb,
        )

    elif text == "🎟 Ввести промокод":
        cancel_kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_promocode")]
            ]
        )
        await message.answer(
            "🎟 <b>Ввод промокода</b>\n\n"
            "Введите промокод, чтобы получить бонусы.\n\n"
            "Отправьте код в следующем сообщении:",
            reply_markup=cancel_kb,
        )
        await state.set_state(PromocodeFlow.awaiting_code)

    elif text == "🔙 Назад в главное меню":
        await message.answer(
            "Главное меню:",
            reply_markup=main_menu_keyboard(),
        )

    else:
        await message.answer(
            "Я вас понял, но не распознал команду 🙂\n"
            "Пожалуйста, используйте кнопки меню ниже.",
            reply_markup=main_menu_keyboard(),
        )


async def handle_review_screenshot(message: Message, state: FSMContext) -> None:
    """
    Пользователь прислал скриншот для бонуса за отзыв (Яндекс или 2ГИС).
    """
    # Проверяем, включён ли бонус за отзыв
    async with AsyncSessionLocal() as session:
        config = await get_bonus_config(session)
        if not config.review_enabled:
            await message.answer(
                "🚫 Бонус за отзыв временно отключён.\n\n"
                "Следите за новостями — скоро он снова станет доступен!",
                reply_markup=loyalty_menu_keyboard(),
            )
            await state.clear()
            return

    if not message.photo:
        await message.answer(
            "Это не похоже на фотографию. Пожалуйста, отправьте именно скриншот опубликованного отзыва."
        )
        return

    file_id = message.photo[-1].file_id
    
    # Определяем платформу по текущему состоянию
    current_state = await state.get_state()
    if current_state and "2gis" in current_state:
        platform = "2gis"
        platform_name = "2ГИС"
    else:
        platform = "yandex"
        platform_name = "Яндекс.Карты"

    async with AsyncSessionLocal() as session:
        user = await get_user_by_telegram_id(session, message.from_user.id)
        if user is None:
            await message.answer(
                "Не удалось найти ваш профиль. Пожалуйста, сначала пройдите регистрацию через /start."
            )
            await state.clear()
            return

        req = ReviewBonusRequest(
            user_id=user.id,
            telegram_user_id=str(message.from_user.id),
            phone=user.phone,
            image_file_id=file_id,
            platform=platform,
        )
        session.add(req)
        await session.commit()
        await session.refresh(req)

        logger.info(
            "ReviewBonusRequest created: user_id=%s telegram_id=%s request_id=%s platform=%s",
            user.id,
            message.from_user.id,
            req.id,
            platform,
        )

    await state.clear()

    await message.answer(
        f"✅ Спасибо! Мы получили скриншот вашего отзыва с <b>{platform_name}</b>.\n\n"
        "Владелец салона проверит его в ближайшее время. "
        "После подтверждения бонусные рубли будут начислены, и вы получите уведомление в этом чате.",
        reply_markup=loyalty_menu_keyboard(),
    )


async def handle_review_non_photo(message: Message, state: FSMContext) -> None:
    """
    Обработка НЕ-фото, когда ждём скриншот отзыва.
    Дружелюбно напоминаем, что нужен именно скрин.
    """
    await message.answer(
        "Сейчас мне нужен именно <b>скриншот опубликованного отзыва</b> 📸\n\n"
        "Пожалуйста, отправьте скрин в виде фото (не текстом и не файлом-документом), "
        "чтобы владельцу салона было удобно его просмотреть и подтвердить бонус."
    )


async def handle_promocode_input(message: Message, state: FSMContext) -> None:
    """
    Обработка ввода промокода.
    """
    code = (message.text or "").strip()
    
    if not code:
        await message.answer(
            "Пожалуйста, введите промокод.",
        )
        return
    
    # Проверяем на команду отмены
    if code.lower() in ("отмена", "назад", "cancel"):
        await state.clear()
        await message.answer(
            "Ввод промокода отменён.",
            reply_markup=loyalty_menu_keyboard(),
        )
        return
    
    async with AsyncSessionLocal() as session:
        user = await get_user_by_telegram_id(session, message.from_user.id)
        if user is None:
            await message.answer(
                "Не удалось найти ваш профиль. Пожалуйста, начните с команды /start.",
                reply_markup=main_menu_keyboard(),
            )
            await state.clear()
            return
        
        success, msg, amount = await apply_promocode(session, user, code)
        
        if success:
            bonus = await get_or_create_user_bonus(session, user)
            await message.answer(
                f"🎉 {msg}\n\n"
                f"Текущий баланс: <b>{bonus.balance}₽</b>",
                reply_markup=loyalty_menu_keyboard(),
            )
            await state.clear()
        else:
            # Остаёмся в состоянии ввода промокода — пользователь может попробовать другой
            cancel_kb = InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_promocode")]
                ]
            )
            await message.answer(
                f"❌ {msg}\n\n"
                "Попробуйте другой промокод или нажмите кнопку ниже:",
                reply_markup=cancel_kb,
            )


async def handle_cancel_start_callback(callback: CallbackQuery) -> None:
    """
    Старт отмены записи из инлайн-кнопки.
    """
    await callback.answer()
    # Лог клика для аналитики (инлайн "Отмена записи")
    try:
        async with AsyncSessionLocal() as session:
            user = await get_user_by_telegram_id(session, callback.from_user.id)
            await log_button_click(
                session=session,
                user_id=user.id if user else None,
                button_name="🗓 Отмена записи (inline)",
            )
    except Exception:
        logger.exception("Failed to log cancel_start click: tg_id=%s", callback.from_user.id)

    await start_cancel_flow(callback.message, callback.from_user.id)


async def handle_cancel_record_callback(callback: CallbackQuery) -> None:
    """
    Обработка inline-кнопок отмены записи:
    - cancel_record:<id>
    - cancel_cancel

    Добавлена проверка: запись действительно принадлежит текущему клиенту в YCLIENTS.
    Плюс логирование успешной отмены в booking_events (для аналитики).
    """
    data = callback.data or ""
    await callback.answer()

    if data == "cancel_cancel":
        await callback.message.edit_text(
            "Окей, ничего отменять не будем 🙂",
        )
        await callback.message.answer(
            "Вы в главном меню.",
            reply_markup=main_menu_keyboard(),
        )
        return

    if data.startswith("cancel_record:"):
        _, record_id_str = data.split(":", 1)
        try:
            record_id = int(record_id_str)
        except ValueError:
            await callback.message.edit_text("Не удалось распознать выбранную запись.")
            await callback.message.answer(
                "Вы в главном меню.",
                reply_markup=main_menu_keyboard(),
            )
            return

        logger.info(
            "Cancel record requested: tg_id=%s record_id=%s",
            callback.from_user.id,
            record_id,
        )

        async with AsyncSessionLocal() as session:
            user = await get_user_by_telegram_id(session, callback.from_user.id)

            if user is None or user.yclients_client_id is None:
                await callback.message.edit_text(
                    "Не удалось найти ваш профиль в базе клиентов YCLIENTS.\n\n"
                    "Пожалуйста, начните заново с команды /start, чтобы привязать номер телефона.",
                )
                await callback.message.answer(
                    "Вы в главном меню.",
                    reply_markup=main_menu_keyboard(),
                )
                return

            yclients_client = YClientsClient()
            record = await yclients_client.get_record_by_id(record_id)

            if not record:
                logger.warning(
                    "Cancel record: record not found or error: tg_id=%s record_id=%s",
                    callback.from_user.id,
                    record_id,
                )
                await callback.message.edit_text(
                    "Не удалось получить данные выбранной записи. Пожалуйста, свяжитесь с администратором.",
                )
                await callback.message.answer(
                    "Вы в главном меню.",
                    reply_markup=main_menu_keyboard(),
                )
                return

            # Пытаемся аккуратно достать client_id записи
            record_client_id = None

            client_block = record.get("client") or record.get("client_info")
            if isinstance(client_block, dict):
                record_client_id = client_block.get("id") or client_block.get("client_id")

            if record_client_id is None:
                record_client_id = record.get("client_id")

            if record_client_id is not None:
                try:
                    record_client_id_int = int(record_client_id)
                except (TypeError, ValueError):
                    record_client_id_int = None
            else:
                record_client_id_int = None

            if (
                record_client_id_int is not None
                and user.yclients_client_id is not None
                and record_client_id_int != int(user.yclients_client_id)
            ):
                logger.warning(
                    "Cancel record denied (client mismatch): tg_id=%s record_id=%s "
                    "record_client_id=%s user_yclients_client_id=%s",
                    callback.from_user.id,
                    record_id,
                    record_client_id_int,
                    user.yclients_client_id,
                )
                await callback.message.edit_text(
                    "Не удалось подтвердить, что эта запись принадлежит вашему профилю.\n\n"
                    "Пожалуйста, свяжитесь с администратором.",
                )
                await callback.message.answer(
                    "Вы в главном меню.",
                    reply_markup=main_menu_keyboard(),
                )
                return

            success = await yclients_client.delete_record(record_id)

            if success:
                logger.info(
                    "Record cancelled: tg_id=%s record_id=%s",
                    callback.from_user.id,
                    record_id,
                )
                # Логируем успешную отмену в booking_events
                try:
                    await log_booking_event(
                        session=session,
                        user=user,
                        event_type=BookingEventType.CANCELLED,
                        yclients_record_id=record_id,
                        meta={"source": "bot"},
                    )
                except Exception:
                    logger.exception(
                        "Failed to log booking cancel event: tg_id=%s user_id=%s record_id=%s",
                        callback.from_user.id,
                        getattr(user, "id", None),
                        record_id,
                    )

                await callback.message.edit_text("Запись успешно отменена ✅")
            else:
                logger.warning(
                    "Record cancel failed in YCLIENTS: tg_id=%s record_id=%s",
                    callback.from_user.id,
                    record_id,
                )
                await callback.message.edit_text(
                    "Не удалось отменить запись. Пожалуйста, свяжитесь с администратором.",
                )

        await callback.message.answer(
            "Вы в главном меню.",
            reply_markup=main_menu_keyboard(),
        )


async def handle_bonus_channel_confirm(callback: CallbackQuery) -> None:
    """
    Callback "✅ Я подписался" — единоразовый бонус за подписку на канал.
    """
    await callback.answer()
    await _log_callback_button(callback, "✅ Я подписался")
    async with AsyncSessionLocal() as session:
        user = await get_user_by_telegram_id(session, callback.from_user.id)
        if user is None:
            await callback.message.edit_text(
                "Не удалось найти ваш профиль. Пожалуйста, начните с команды /start."
            )
            return

        config = await get_bonus_config(session)
        
        # Проверяем, включён ли бонус за подписку
        if not config.channel_enabled:
            await callback.message.edit_text(
                "🚫 Бонус за подписку на канал временно отключён.\n\n"
                "Следите за новостями — скоро он снова станет доступен!"
            )
            return
        
        channel_amount = config.channel_amount

        bonus, granted = await try_add_fixed_bonus(
            session=session,
            user=user,
            flag_field="channel_given",
            amount=channel_amount,
        )

    if granted:
        await callback.message.edit_text(
            f"📢 Спасибо за подписку!\n"
            f"Начислено <b>{channel_amount}₽</b>.\n"
            f"Текущий баланс: <b>{bonus.balance}₽</b>."
        )
    else:
        await callback.message.edit_text(
            "Похоже, бонус за подписку уже был начислен ранее 🙂\n"
            f"Текущий баланс: <b>{bonus.balance}₽</b>."
        )


async def handle_bonus_review(callback: CallbackQuery) -> None:
    """
    СТАРЫЙ сценарий callback'ов:
    - bonus_review_yandex
    - bonus_review_2gis

    Сейчас UI больше не даёт сюда попасть (нет inline-кнопок),
    но оставляем на будущее, если захотим доверительную схему для 2ГИС.
    """
    data = callback.data or ""
    await callback.answer()

    if data not in ("bonus_review_yandex", "bonus_review_2gis"):
        return

    flag_field = "review_yandex_given" if data == "bonus_review_yandex" else "review_2gis_given"
    place_name = "Яндекс" if data == "bonus_review_yandex" else "2ГИС"

    async with AsyncSessionLocal() as session:
        user = await get_user_by_telegram_id(session, callback.from_user.id)
        if user is None:
            await callback.message.edit_text(
                "Не удалось найти ваш профиль. Пожалуйста, начните с команды /start."
            )
            return

        config = await get_bonus_config(session)
        review_amount = config.review_amount

        bonus, granted = await try_add_fixed_bonus(
            session=session,
            user=user,
            flag_field=flag_field,
            amount=review_amount,
        )

    if granted:
        await callback.message.edit_text(
            f"⭐ Спасибо за отзыв на <b>{place_name}</b>!\n"
            f"Начислено <b>{review_amount}₽</b>.\n"
            f"Текущий баланс: <b>{bonus.balance}₽</b>."
        )
    else:
        await callback.message.edit_text(
            f"Бонус за отзыв на <b>{place_name}</b> уже был начислен ранее 🙂\n"
            f"Текущий баланс: <b>{bonus.balance}₽</b>."
        )


async def handle_review_platform_callback(callback: CallbackQuery, state: FSMContext) -> None:
    """
    Callback для выбора платформы отзыва: Яндекс или 2ГИС.
    """
    data = callback.data or ""
    await callback.answer()
    
    platform = "Яндекс" if "yandex" in data else "2ГИС"
    await _log_callback_button(callback, f"Отзыв на {platform}")

    # Проверяем, включён ли бонус за отзыв
    async with AsyncSessionLocal() as session:
        config = await get_bonus_config(session)
        if not config.review_enabled:
            await callback.message.edit_text(
                "🚫 Бонус за отзыв временно отключён.\n\n"
                "Следите за новостями — скоро он снова станет доступен!"
            )
            return

    if data == "review_platform_yandex":
        platform_name = "Яндекс.Карты"
        await state.set_state(ReviewFlow.awaiting_yandex_screenshot)
    elif data == "review_platform_2gis":
        platform_name = "2ГИС"
        await state.set_state(ReviewFlow.awaiting_2gis_screenshot)
    else:
        return

    instructions = (
        f"⭐ <b>Бонус за отзыв на {platform_name}</b>\n\n"
        f"1️⃣ Напишите отзыв о салоне на {platform_name}.\n"
        "2️⃣ Дождитесь, когда он пройдёт модерацию и будет опубликован.\n"
        "3️⃣ Сделайте скриншот экрана, где видно:\n"
        "   • название салона\n"
        "   • вашу оценку\n"
        "   • текст отзыва.\n"
        "4️⃣ Просто <b>отправьте скриншот как фотографию</b> в этот чат.\n\n"
        "Важно: бот принимает <b>только фото</b> — текст и документы не засчитываются.\n\n"
        "После модерации владельцем салона бонусные рубли будут начислены на ваш счёт."
    )

    await callback.message.answer(instructions)


async def handle_cancel_promocode(callback: CallbackQuery, state: FSMContext) -> None:
    """Отмена ввода промокода по inline-кнопке."""
    await callback.answer()
    await state.clear()
    await callback.message.answer(
        "Ввод промокода отменён.",
        reply_markup=loyalty_menu_keyboard(),
    )


async def handle_misc_callbacks(callback: CallbackQuery) -> None:
    """
    Прочие callback'и:
    - open_ref_program
    - get_ref_link
    """
    data = callback.data or ""
    await callback.answer()
    
    label = "Реферальная программа" if data == "open_ref_program" else "Получить реф. ссылку"
    await _log_callback_button(callback, label)

    if data == "open_ref_program":
        # Показать блок "Реферальная программа" так же, как при нажатии кнопки меню
        async with AsyncSessionLocal() as session:
            user = await get_user_by_telegram_id(session, callback.from_user.id)
            if user is None:
                await callback.message.answer(
                    "Не удалось найти ваш профиль. Пожалуйста, начните с команды /start.",
                )
                return

            bonus = await get_or_create_user_bonus(session, user)
            balance = bonus.balance

            config = await get_bonus_config(session)
            referral_amount = config.referral_amount

        me = await callback.message.bot.get_me()
        deep_link = f"https://t.me/{me.username}?start=ref_{bonus.referral_code}"

        kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text="🔗 Получить реферальную ссылку",
                        callback_data="get_ref_link",
                    )
                ]
            ]
        )

        await send_media_with_fallback(
            callback.message,
            media_path="media/gifs/10_referral_program.mp4",
            photo_fallback_path="",  # Нет fallback фото
            caption=(
                "🤝 <b>Реферальная программа</b>\n\n"
                f"Поделитесь реферальной ссылкой с другом — за приглашение вы получите "
                f"<b>{referral_amount}₽</b> бонусных рублей, если друг воспользуется любой услугой "
                "в Ashstyle Barber Lounge.\n\n"
                "Ниже вы можете запросить свою персональную ссылку.\n\n"
                f"Текущий баланс: <b>{balance}₽</b>."
            ),
            reply_markup=kb,
        )
        return

    if data == "get_ref_link":
        async with AsyncSessionLocal() as session:
            user = await get_user_by_telegram_id(session, callback.from_user.id)
            if user is None:
                await callback.message.answer(
                    "Не удалось найти ваш профиль. Пожалуйста, начните с команды /start."
                )
                return

            bonus = await get_or_create_user_bonus(session, user)

        me = await callback.message.bot.get_me()
        deep_link = f"https://t.me/{me.username}?start=ref_{bonus.referral_code}"

        await callback.message.answer(
            f"Ваша реферальная ссылка:\n{deep_link}"
        )


async def main() -> None:
    global _bot_instance
    
    logging.basicConfig(
        level=getattr(logging, settings.LOG_LEVEL.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    logger.info("Инициализация БД...")
    await init_db()
    logger.info("БД инициализирована.")

    bot = Bot(
        token=settings.BOT_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )
    _bot_instance = bot  # Сохраняем для использования в scheduler
    
    dp = Dispatcher(storage=MemoryStorage())

    # Запускаем планировщик для автоматической проверки реферальных бонусов
    scheduler = AsyncIOScheduler()
    scheduler.add_job(
        check_referral_bonuses_job,
        "interval",
        minutes=10,  # Проверка каждые 10 минут
        id="referral_bonus_check",
        replace_existing=True,
    )
    scheduler.start()
    logger.info("APScheduler started: referral bonus check every 10 minutes")

    # Регистрация хендлеров
    dp.message.register(handle_start, CommandStart())
    dp.callback_query.register(on_consent_accepted, F.data == "consent_accept")

    # Пока ждём согласия — на любое текстовое сообщение даём подсказку "нажмите Согласен"
    dp.message.register(
        handle_text_awaiting_consent,
        StateFilter(Registration.awaiting_consent),
    )

    # Контакт обрабатываем с учётом состояния/согласия (см. process_contact)
    dp.message.register(process_contact, F.contact)

    # Ручной ввод телефона:
    # - когда явно ждём телефон
    dp.message.register(process_phone_text, Registration.awaiting_phone)
    # - и после рестарта, когда state=None, но текст похож на номер
    dp.message.register(
        process_phone_text,
        StateFilter(None),
        F.text.regexp(r"^\+?\d[\d\-\s\(\)]{8,}$"),
    )

    # Скриншот для бонуса за отзыв (Яндекс)
    dp.message.register(
        handle_review_screenshot,
        StateFilter(ReviewFlow.awaiting_yandex_screenshot),
        F.photo,
    )
    # Скриншот для бонуса за отзыв (2ГИС)
    dp.message.register(
        handle_review_screenshot,
        StateFilter(ReviewFlow.awaiting_2gis_screenshot),
        F.photo,
    )
    # Любое НЕ-фото, пока ждём скриншот (Яндекс)
    dp.message.register(
        handle_review_non_photo,
        StateFilter(ReviewFlow.awaiting_yandex_screenshot),
    )
    # Любое НЕ-фото, пока ждём скриншот (2ГИС)
    dp.message.register(
        handle_review_non_photo,
        StateFilter(ReviewFlow.awaiting_2gis_screenshot),
    )
    
    # Ввод промокода
    dp.message.register(
        handle_promocode_input,
        StateFilter(PromocodeFlow.awaiting_code),
    )

    # Главное меню доступно ТОЛЬКО когда нет активного состояния FSM
    dp.message.register(handle_main_menu, StateFilter(None))

    dp.callback_query.register(handle_cancel_start_callback, F.data == "cancel_start")
    dp.callback_query.register(handle_cancel_record_callback, F.data.startswith("cancel_record:"))
    dp.callback_query.register(handle_cancel_record_callback, F.data == "cancel_cancel")

    dp.callback_query.register(handle_bonus_channel_confirm, F.data == "bonus_channel_confirm")
    dp.callback_query.register(
        handle_bonus_review,
        (F.data == "bonus_review_yandex") | (F.data == "bonus_review_2gis"),
    )
    
    # Выбор платформы для отзыва
    dp.callback_query.register(
        handle_review_platform_callback,
        (F.data == "review_platform_yandex") | (F.data == "review_platform_2gis"),
    )

    dp.callback_query.register(
        handle_misc_callbacks,
        (F.data == "open_ref_program") | (F.data == "get_ref_link"),
    )
    
    dp.callback_query.register(
        handle_cancel_promocode,
        F.data == "cancel_promocode",
    )

    logger.info("Старт long polling...")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())

