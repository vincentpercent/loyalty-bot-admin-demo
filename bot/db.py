from datetime import datetime
from typing import Optional
import secrets
import string
import logging
import enum

from sqlalchemy import Enum as SAEnum
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    DateTime,
    Integer,
    String,
    select,
    Enum,
    ForeignKey,
    Numeric,
    Text,
)
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import declarative_base, relationship

from config import settings

Base = declarative_base()
logger = logging.getLogger(__name__)


class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)
    telegram_id = Column(BigInteger, unique=True, index=True, nullable=False)
    username = Column(String, nullable=True)
    full_name = Column(String, nullable=True)

    phone = Column(String, nullable=True)
    yclients_client_id = Column(Integer, nullable=True)

    # None = мы ещё не определяли "новый/старый" по YCLIENTS
    is_new_client = Column(Boolean, nullable=True)

    agreed_privacy = Column(Boolean, default=False)

    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class UserBonus(Base):
    """
    Таблица для бонусного баланса и флагов.
    """
    __tablename__ = "user_bonuses"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, index=True, nullable=False)

    balance = Column(Integer, default=0)

    welcome_given = Column(Boolean, default=False)
    channel_given = Column(Boolean, default=False)
    review_yandex_given = Column(Boolean, default=False)
    review_2gis_given = Column(Boolean, default=False)

    referral_code = Column(String, unique=True, nullable=True)
    referred_by_code = Column(String, nullable=True)
    referral_earned = Column(Integer, default=0)

    referred_registration_notified = Column(Boolean, default=False)
    referral_visit_reward_given = Column(Boolean, default=False)

    referral_bound_at = Column(DateTime, nullable=True)
    
    # Флаг: отправляли ли уведомление с предложением написать отзыв после первого визита
    first_visit_review_notified = Column(Boolean, default=False)

    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class ReviewRequestStatus(str, enum.Enum):
    NEW = "NEW"
    CONFIRMED = "CONFIRMED"
    REJECTED = "REJECTED"


class ReviewPlatform(str, enum.Enum):
    YANDEX = "yandex"
    TWOGIS = "2gis"


class ReviewBonusRequest(Base):
    """
    Заявка на бонус за отзыв (со скриншотом).
    """
    __tablename__ = "review_bonus_requests"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)

    telegram_user_id = Column(String, nullable=False)
    phone = Column(String, nullable=True)

    image_file_id = Column(String, nullable=False)
    
    # Платформа отзыва (yandex / 2gis)
    platform = Column(
        String,
        default="yandex",
        nullable=False,
    )

    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    status = Column(
        Enum(ReviewRequestStatus),
        default=ReviewRequestStatus.NEW,
        nullable=False,
        index=True,
    )
    decided_at = Column(DateTime, nullable=True)
    decided_by = Column(String, nullable=True)
    decision_comment = Column(Text, nullable=True)

    bonus_transaction_id = Column(
        Integer,
        ForeignKey("bonus_transactions.id"),
        nullable=True,
    )

    user = relationship("User", backref="review_requests")
    bonus_transaction = relationship("BonusTransaction", backref="review_request")


class BonusTransactionType(str, enum.Enum):
    ACCRUAL = "ACCRUAL"
    DEBIT = "DEBIT"


class BonusTransactionSource(str, enum.Enum):
    REVIEW = "REVIEW"
    WELCOME = "WELCOME"
    REFERRAL = "REFERRAL"
    SUBSCRIPTION = "SUBSCRIPTION"
    MANUAL = "MANUAL"
    PROMOCODE = "PROMOCODE"


class BonusTransaction(Base):
    """
    История движения бонусов.
    """
    __tablename__ = "bonus_transactions"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)

    amount = Column(Numeric(10, 2), nullable=False)  # +/-
    type = Column(Enum(BonusTransactionType), nullable=False)
    source = Column(Enum(BonusTransactionSource), nullable=False)

    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    created_by = Column(String, nullable=True)
    comment = Column(Text, nullable=True)

    user = relationship("User", backref="bonus_transactions")


class AuditLog(Base):
    """
    Аудит действий администратора в панели.
    """
    __tablename__ = "admin_audit_log"

    id = Column(Integer, primary_key=True, index=True)
    admin_username = Column(String, nullable=False)
    action = Column(String, nullable=False)
    entity_type = Column(String, nullable=True)
    entity_id = Column(Integer, nullable=True)
    payload = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)


class BotText(Base):
    """
    Редактируемые тексты бота.
    key — уникальный идентификатор текста (например: welcome_message, review_prompt)
    value — текст (поддерживает HTML-разметку)
    """
    __tablename__ = "bot_texts"

    id = Column(Integer, primary_key=True, index=True)
    key = Column(String, unique=True, nullable=False, index=True)
    value = Column(Text, nullable=False)
    description = Column(String, nullable=True)  # Описание для админки
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class Promocode(Base):
    """
    Промокоды для начисления бонусов.
    """
    __tablename__ = "promocodes"

    id = Column(Integer, primary_key=True, index=True)
    code = Column(String, unique=True, nullable=False, index=True)  # Уникальный код
    bonus_amount = Column(Integer, nullable=False)  # Сумма бонуса
    
    max_uses = Column(Integer, default=0)  # Макс. использований (0 = бесконечно)
    current_uses = Column(Integer, default=0)  # Текущее кол-во использований
    
    valid_from = Column(DateTime, nullable=True)  # Начало действия
    valid_to = Column(DateTime, nullable=True)  # Конец действия
    
    is_active = Column(Boolean, default=True, nullable=False)
    description = Column(String, nullable=True)  # Описание для админки
    
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class PromocodeUsage(Base):
    """
    История использования промокодов.
    """
    __tablename__ = "promocode_usages"

    id = Column(Integer, primary_key=True, index=True)
    promocode_id = Column(Integer, ForeignKey("promocodes.id"), nullable=False)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    bonus_amount = Column(Integer, nullable=False)
    used_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    
    promocode = relationship("Promocode", backref="usages")
    user = relationship("User", backref="promocode_usages")


class BonusConfig(Base):
    """
    Глобальные настройки бонусной программы.
    В БД уже есть created_at NOT NULL — поэтому явно описываем его.
    """
    __tablename__ = "bonus_config"

    id = Column(Integer, primary_key=True, index=True)

    # Суммы бонусов
    welcome_amount = Column(Integer, default=500, nullable=False)
    channel_amount = Column(Integer, default=500, nullable=False)
    review_amount = Column(Integer, default=500, nullable=False)
    referral_amount = Column(Integer, default=500, nullable=False)

    # Статусы заданий (включено/выключено)
    welcome_enabled = Column(Boolean, default=True, nullable=False)
    channel_enabled = Column(Boolean, default=True, nullable=False)
    review_enabled = Column(Boolean, default=True, nullable=False)
    referral_enabled = Column(Boolean, default=True, nullable=False)

    # Правила списания
    max_bonus_percent = Column(Integer, default=30, nullable=False)  # макс % оплаты бонусами

    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)


DATABASE_URL = (
    f"postgresql+asyncpg://{settings.db.user}:"
    f"{settings.db.password}@{settings.db.host}:"
    f"{settings.db.port}/{settings.db.database}"
)

engine = create_async_engine(DATABASE_URL, echo=False, future=True)
AsyncSessionLocal = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)


# =========================
#   BOOKING EVENTS: HELPER
# =========================

async def log_booking_event(
    *,
    event_type: "BookingEventType",
    yclients_booking_id: str | None = None,
    telegram_user_id: int | None = None,
    client_phone: str | None = None,
    status: str | None = None,
    source: str | None = None,
    meta: dict | None = None,
) -> None:
    """
    Универсальный helper для записи событий по записям.

    Примеры использования из кода бота:

        await log_booking_event(
            event_type=BookingEventType.CREATED,
            yclients_booking_id=str(booking_id),
            telegram_user_id=user.telegram_id,
            client_phone=user.phone,
            status="created",
            source="bot",
            meta={
                "master_id": master_id,
                "service_ids": service_ids,
            },
        )

    """
    async with AsyncSessionLocal() as session:
        ev = BookingEvent(
            type=event_type,
            yclients_booking_id=yclients_booking_id,
            telegram_user_id=telegram_user_id,
            client_phone=client_phone,
            status=status,
            source=source,
            meta=meta or {},
        )
        session.add(ev)
        await session.commit()



async def init_db() -> None:
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


async def get_bonus_config(session: AsyncSession) -> BonusConfig:
    """
    Возвращает единственную строку с конфигом бонусов.
    Если её нет — создаёт с дефолтами.
    """
    cfg = await session.get(BonusConfig, 1)
    if cfg is None:
        cfg = BonusConfig(
            id=1,
            welcome_amount=500,
            channel_amount=500,
            review_amount=500,
            referral_amount=500,
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow(),
        )
        session.add(cfg)
        await session.commit()
        await session.refresh(cfg)
        logger.info(
            "BonusConfig created with defaults: welcome=%s channel=%s review=%s referral=%s",
            cfg.welcome_amount,
            cfg.channel_amount,
            cfg.review_amount,
            cfg.referral_amount,
        )
    return cfg


# Дефолтные тексты бота
DEFAULT_BOT_TEXTS = {
    "welcome_message": {
        "value": "👋 Добро пожаловать в <b>Demo Lounge</b>\n\nПеред тем как продолжить, важно подтвердить согласие с документами.",
        "description": "Приветственное сообщение при /start",
    },
    "welcome_bonus_new": {
        "value": "🎉 Welcome bonus начислен: <b>{amount}₽</b>.\nТекущий баланс: <b>{balance}₽</b>.",
        "description": "Сообщение при начислении welcome bonus (новому клиенту)",
    },
    "welcome_bonus_old": {
        "value": "Welcome bonus доступен только новым клиентам, ранее не посещавшим Ashstyle Barber Lounge.\n\nПоделитесь реферальной ссылкой с другом — за приглашение вы получите <b>{referral_amount}₽</b> бонусных рублей.",
        "description": "Сообщение при попытке получить welcome bonus обычным клиентом",
    },
    "first_visit_review_prompt": {
        "value": "🎉 <b>Спасибо за визит!</b>\n\nМы видим, что вы недавно посещали салон. Будем благодарны, если оставите отзыв на Яндекс.Картах или 2ГИС!\n\nЗа каждый подтверждённый отзыв вы получите бонусные рубли.",
        "description": "Уведомление после первого визита с предложением оставить отзыв",
    },
    "referral_invite_text": {
        "value": "🤝 <b>Реферальная программа</b>\n\nПоделитесь реферальной ссылкой с другом — за приглашение вы получите <b>{referral_amount}₽</b> бонусных рублей, если друг воспользуется любой услугой в Ashstyle Barber Lounge.",
        "description": "Текст в разделе реферальной программы",
    },
}


async def get_bot_text(session: AsyncSession, key: str) -> str:
    """
    Получить текст бота по ключу.
    Если текста нет в БД — возвращает дефолтное значение.
    """
    result = await session.execute(
        select(BotText).where(BotText.key == key)
    )
    text_row = result.scalar_one_or_none()
    
    if text_row:
        return text_row.value
    
    # Возвращаем дефолт если нет в БД
    default = DEFAULT_BOT_TEXTS.get(key)
    if default:
        return default["value"]
    
    return f"[Текст '{key}' не найден]"


async def init_bot_texts(session: AsyncSession) -> None:
    """
    Инициализирует тексты бота дефолтными значениями, если их нет.
    """
    for key, data in DEFAULT_BOT_TEXTS.items():
        result = await session.execute(
            select(BotText).where(BotText.key == key)
        )
        existing = result.scalar_one_or_none()
        if not existing:
            text = BotText(
                key=key,
                value=data["value"],
                description=data.get("description"),
            )
            session.add(text)
    await session.commit()


async def apply_promocode(
    session: AsyncSession,
    user: "User",
    code: str,
) -> tuple[bool, str, int]:
    """
    Применить промокод для пользователя.
    
    Возвращает (success, message, bonus_amount):
    - success: True если промокод применён
    - message: сообщение для пользователя
    - bonus_amount: сумма начисленных бонусов (0 если не применён)
    """
    # Нормализуем код (убираем пробелы, приводим к верхнему регистру)
    code = code.strip().upper()
    
    # Ищем промокод
    result = await session.execute(
        select(Promocode).where(Promocode.code == code)
    )
    promo = result.scalar_one_or_none()
    
    if not promo:
        return False, "Промокод не найден", 0
    
    if not promo.is_active:
        return False, "Промокод неактивен", 0
    
    now = datetime.utcnow()
    
    # Проверяем срок действия
    if promo.valid_from and now < promo.valid_from:
        return False, "Промокод ещё не активен", 0
    
    if promo.valid_to and now > promo.valid_to:
        return False, "Срок действия промокода истёк", 0
    
    # Проверяем лимит использований
    if promo.max_uses > 0 and promo.current_uses >= promo.max_uses:
        return False, "Промокод исчерпан", 0
    
    # Проверяем, не использовал ли пользователь этот промокод ранее
    usage_result = await session.execute(
        select(PromocodeUsage).where(
            PromocodeUsage.promocode_id == promo.id,
            PromocodeUsage.user_id == user.id,
        )
    )
    existing_usage = usage_result.scalar_one_or_none()
    
    if existing_usage:
        return False, "Вы уже использовали этот промокод", 0
    
    # Применяем промокод
    bonus = await get_or_create_user_bonus(session, user)
    bonus.balance += promo.bonus_amount
    
    # Записываем использование
    usage = PromocodeUsage(
        promocode_id=promo.id,
        user_id=user.id,
        bonus_amount=promo.bonus_amount,
    )
    session.add(usage)
    
    # Увеличиваем счётчик использований
    promo.current_uses += 1
    
    # Создаём транзакцию
    tx = BonusTransaction(
        user_id=user.id,
        amount=promo.bonus_amount,
        type=BonusTransactionType.ACCRUAL,
        source=BonusTransactionSource.PROMOCODE,
        comment=f"Промокод {code}",
    )
    session.add(tx)
    
    await session.commit()
    await session.refresh(bonus)
    
    # Синхронизируем баланс в YClients
    await sync_bonus_to_yclients(user, bonus.balance, delta=promo.bonus_amount)
    
    return True, f"Промокод применён! Начислено {promo.bonus_amount}₽", promo.bonus_amount


async def get_user_by_telegram_id(session: AsyncSession, telegram_id: int) -> User | None:
    result = await session.execute(
        select(User).where(User.telegram_id == telegram_id)
    )
    return result.scalar_one_or_none()


async def get_user_by_yclients_id(
    session: AsyncSession,
    yclients_client_id: int,
) -> User | None:
    """
    Находит пользователя по yclients_client_id.
    """
    result = await session.execute(
        select(User)
        .where(User.yclients_client_id == yclients_client_id)
        .limit(1)
    )
    return result.scalar_one_or_none()


async def create_or_update_user(
    session: AsyncSession,
    telegram_id: int,
    username: str | None,
    full_name: str | None,
    phone: str | None = None,
    agreed_privacy: bool | None = None,
    yclients_client_id: int | None = None,
    is_new_client: bool | None = None,
) -> User:
    """
    Создание или обновление пользователя.
    """
    user = await get_user_by_telegram_id(session, telegram_id)

    is_new_before = None
    if user is not None:
        is_new_before = user.is_new_client

    if user is None:
        user = User(
            telegram_id=telegram_id,
            username=username,
            full_name=full_name,
        )
        session.add(user)

    if phone is not None:
        user.phone = phone

    if agreed_privacy is not None:
        user.agreed_privacy = agreed_privacy

    if yclients_client_id is not None:
        user.yclients_client_id = yclients_client_id

    if is_new_client is not None:
        if user.is_new_client is None:
            user.is_new_client = is_new_client

    await session.commit()
    await session.refresh(user)

    logger.info(
        "User upserted: telegram_id=%s user_id=%s phone=%s yclients_client_id=%s "
        "is_new_client(before)=%s is_new_client(now)=%s agreed_privacy=%s",
        telegram_id,
        user.id,
        user.phone,
        user.yclients_client_id,
        is_new_before,
        user.is_new_client,
        user.agreed_privacy,
    )

    return user


def _generate_referral_code(length: int = 8) -> str:
    alphabet = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"
    return "".join(secrets.choice(alphabet) for _ in range(length))


async def get_user_bonus_by_user_id(session: AsyncSession, user_id: int) -> UserBonus | None:
    result = await session.execute(
        select(UserBonus).where(UserBonus.user_id == user_id)
    )
    return result.scalar_one_or_none()


async def get_user_bonus_by_referral_code(session: AsyncSession, code: str) -> UserBonus | None:
    result = await session.execute(
        select(UserBonus).where(UserBonus.referral_code == code)
    )
    return result.scalar_one_or_none()


async def get_or_create_user_bonus(session: AsyncSession, user: User) -> UserBonus:
    bonus = await get_user_bonus_by_user_id(session, user.id)
    if bonus is None:
        bonus = UserBonus(
            user_id=user.id,
            balance=0,
            referral_code=_generate_referral_code(),
        )
        session.add(bonus)
        await session.commit()
        await session.refresh(bonus)
        logger.info(
            "UserBonus created: user_id=%s bonus_id=%s referral_code=%s",
            user.id,
            bonus.id,
            bonus.referral_code,
        )
    return bonus


async def try_add_fixed_bonus(
    session: AsyncSession,
    user: User,
    flag_field: str,
    amount: int,
) -> tuple[UserBonus, bool]:
    """
    Начисляет фиксированный бонус 'amount', если флаг flag_field ещё False.
    Также создаёт транзакцию для отслеживания истории.
    """
    bonus = await get_or_create_user_bonus(session, user)

    already = getattr(bonus, flag_field, None)
    if already:
        logger.info(
            "Bonus not granted (already given): user_id=%s flag=%s balance=%s",
            user.id,
            flag_field,
            bonus.balance,
        )
        return bonus, False

    setattr(bonus, flag_field, True)
    bonus.balance += amount

    # Определяем источник транзакции по флагу
    source_map = {
        "welcome_given": BonusTransactionSource.WELCOME,
        "channel_given": BonusTransactionSource.SUBSCRIPTION,
        "review_yandex_given": BonusTransactionSource.REVIEW,
        "review_2gis_given": BonusTransactionSource.REVIEW,
    }
    source = source_map.get(flag_field)
    
    # Создаём транзакцию
    if source:
        comment_map = {
            "welcome_given": "Welcome bonus",
            "channel_given": "Бонус за подписку на канал",
            "review_yandex_given": "Бонус за отзыв на Яндекс",
            "review_2gis_given": "Бонус за отзыв на 2ГИС",
        }
        tx = BonusTransaction(
            user_id=user.id,
            amount=amount,
            type=BonusTransactionType.ACCRUAL,
            source=source,
            created_at=datetime.utcnow(),
            comment=comment_map.get(flag_field, "Бонус"),
        )
        session.add(tx)

    await session.commit()
    await session.refresh(bonus)

    # Синхронизируем баланс в YClients
    await sync_bonus_to_yclients(user, bonus.balance, delta=amount)

    logger.info(
        "Bonus granted: user_id=%s flag=%s amount=%s new_balance=%s",
        user.id,
        flag_field,
        amount,
        bonus.balance,
    )
    return bonus, True


async def apply_referral_if_needed(
    session: AsyncSession,
    user: User,
    referral_code: str | None,
) -> None:
    """
    Привязка реферального кода к приглашённому пользователю.
    Бонусы за визит потом, отдельной функцией.
    
    ВАЖНО: Реферальный код привязывается ТОЛЬКО для новых клиентов YClients.
    Старые клиенты (is_new_client=False) не могут использовать реферальные ссылки.
    """
    if not referral_code:
        return

    # Проверяем, что это новый клиент YClients
    if not user.is_new_client:
        logger.info(
            "Referral code ignored: user_id=%s referral_code=%s reason=not_new_yclients_client",
            user.id,
            referral_code,
        )
        return

    invited_bonus = await get_or_create_user_bonus(session, user)

    if invited_bonus.referred_by_code:
        logger.info(
            "Referral code ignored: user_id=%s already has referred_by_code=%s",
            user.id,
            invited_bonus.referred_by_code,
        )
        return

    inviter_bonus = await get_user_bonus_by_referral_code(session, referral_code)
    if inviter_bonus is None:
        logger.info(
            "Referral code ignored: user_id=%s referral_code=%s not found",
            user.id,
            referral_code,
        )
        return

    if inviter_bonus.user_id == user.id:
        logger.info(
            "Referral code ignored (self-referral): user_id=%s referral_code=%s",
            user.id,
            referral_code,
        )
        return

    invited_bonus.referred_by_code = referral_code
    invited_bonus.referral_bound_at = datetime.utcnow()

    await session.commit()

    logger.info(
        "Referral bound: invited_user_id=%s inviter_user_id=%s referral_code=%s bound_at=%s",
        user.id,
        inviter_bonus.user_id,
        referral_code,
        invited_bonus.referral_bound_at,
    )


async def reward_referral_after_visit(
    session: AsyncSession,
    user: User,
    amount: int,
    visit_dt: datetime | None,
) -> tuple[UserBonus | None, UserBonus | None, bool]:
    """
    Реферальный бонус отправителю ПОСЛЕ визита приглашённого.
    """
    invited_bonus = await get_or_create_user_bonus(session, user)

    if not invited_bonus.referred_by_code:
        logger.info(
            "Referral reward not granted: user_id=%s reason=no_ref_code",
            user.id,
        )
        return invited_bonus, None, False

    if invited_bonus.referral_visit_reward_given:
        logger.info(
            "Referral reward not granted: user_id=%s reason=already_given",
            user.id,
        )
        return invited_bonus, None, False

    if not user.is_new_client:
        logger.info(
            "Referral reward not granted: user_id=%s reason=not_new_client",
            user.id,
        )
        return invited_bonus, None, False

    if visit_dt is not None and invited_bonus.referral_bound_at is not None:
        if visit_dt < invited_bonus.referral_bound_at:
            logger.info(
                "Referral reward not granted: user_id=%s reason=visit_before_binding visit_dt=%s bound_at=%s",
                user.id,
                visit_dt,
                invited_bonus.referral_bound_at,
            )
            return invited_bonus, None, False

    inviter_bonus = await get_user_bonus_by_referral_code(
        session,
        invited_bonus.referred_by_code,
    )
    if inviter_bonus is None:
        logger.info(
            "Referral reward not granted: user_id=%s reason=inviter_not_found code=%s",
            user.id,
            invited_bonus.referred_by_code,
        )
        return invited_bonus, None, False

    if inviter_bonus.user_id == user.id:
        logger.info(
            "Referral reward not granted: user_id=%s reason=self_referral",
            user.id,
        )
        return invited_bonus, None, False

    inviter_bonus.balance += amount
    inviter_bonus.referral_earned += amount

    invited_bonus.referral_visit_reward_given = True

    # Создаём транзакцию для реферального бонуса
    tx = BonusTransaction(
        user_id=inviter_bonus.user_id,
        amount=amount,
        type=BonusTransactionType.ACCRUAL,
        source=BonusTransactionSource.REFERRAL,
        created_at=datetime.utcnow(),
        comment=f"Реферальный бонус за приглашение пользователя ID {user.id}",
    )
    session.add(tx)

    await session.commit()
    await session.refresh(invited_bonus)
    await session.refresh(inviter_bonus)

    # Синхронизируем баланс инвайтера в YClients
    inviter_user = await session.get(User, inviter_bonus.user_id)
    if inviter_user:
        await sync_bonus_to_yclients(inviter_user, inviter_bonus.balance, delta=amount)

    logger.info(
        "Referral reward granted: inviter_user_id=%s invited_user_id=%s amount=%s inviter_balance=%s",
        inviter_bonus.user_id,
        user.id,
        amount,
        inviter_bonus.balance,
    )

    return invited_bonus, inviter_bonus, True


async def another_user_has_welcome_for_client(
    session: AsyncSession,
    yclients_client_id: int,
    current_user_id: int,
) -> bool:
    """
    Проверка, что welcome за этого клиента уже выдавался другому пользователю.
    """
    if yclients_client_id is None:
        return False

    from sqlalchemy import func  # локальный импорт, чтобы не засорять сверху

    stmt = (
        select(UserBonus)
        .join(User, User.id == UserBonus.user_id)
        .where(
            User.yclients_client_id == yclients_client_id,
            UserBonus.welcome_given.is_(True),
            User.id != current_user_id,
        )
        .limit(1)
    )

    result = await session.execute(stmt)
    existing = result.scalar_one_or_none()

    if existing is not None:
        logger.info(
            "another_user_has_welcome_for_client: yclients_client_id=%s current_user_id=%s welcome_already_given_by_user_id=%s",
            yclients_client_id,
            current_user_id,
            existing.user_id,
        )
        return True

    return False


async def get_pending_referral_users(session: AsyncSession) -> list[tuple[User, UserBonus]]:
    """
    Получить пользователей, ожидающих проверку реферального бонуса:
    - referred_by_code установлен (пришли по реферальной ссылке)
    - referral_visit_reward_given = False (бонус ещё не начислен)
    - is_new_client = True (новый клиент YClients)
    - yclients_client_id установлен (можно проверить визит)
    """
    stmt = (
        select(User, UserBonus)
        .join(UserBonus, UserBonus.user_id == User.id)
        .where(
            UserBonus.referred_by_code.isnot(None),
            UserBonus.referral_visit_reward_given.is_(False),
            User.is_new_client.is_(True),
            User.yclients_client_id.isnot(None),
        )
    )
    
    result = await session.execute(stmt)
    return list(result.all())


class BookingEventType(enum.Enum):
    CLICK_BOOKING = "CLICK_BOOKING"   # нажата кнопка/меню записи в боте
    CREATED = "CREATED"               # запись создана (если заведём запись у себя)
    COMPLETED = "COMPLETED"           # визит состоялся
    CANCELLED = "CANCELLED"           # запись отменена


class BookingEvent(Base):
    """
    События по записям, чтобы строить аналитику по ТЗ
    вообще без привязки к YCLIENTS.
    """
    __tablename__ = "booking_events"

    id = Column(Integer, primary_key=True)

    user_id = Column(
        Integer,
        ForeignKey("users.id"),
        nullable=True,
        index=True,
    )

    event_type = Column(
        SAEnum(BookingEventType),
        nullable=False,
        index=True,
    )

    created_at = Column(
        DateTime,
        nullable=False,
        default=datetime.utcnow,
        index=True,
    )

    # любые дополнительные данные, если пригодятся:
    # yclients_id, источник, из какого экрана пришёл и т.п.
    meta = Column(JSONB, nullable=True)

    user = relationship("User", backref="booking_events")


# =========================
#   АНАЛИТИКА КНОПОК
# =========================

class ButtonEvent(Base):
    """
    Логирование нажатий на кнопки в боте для аналитики.
    """
    __tablename__ = "button_events"

    id = Column(Integer, primary_key=True)

    user_id = Column(
        Integer,
        ForeignKey("users.id"),
        nullable=True,
        index=True,
    )

    button_name = Column(
        String,
        nullable=False,
        index=True,
    )

    created_at = Column(
        DateTime,
        nullable=False,
        default=datetime.utcnow,
        index=True,
    )

    user = relationship("User", backref="button_events")


class MediaFileCache(Base):
    """
    Кэш для file_id медиа-файлов (GIF, фото) от Telegram.
    После первой отправки файла Telegram возвращает file_id,
    который можно использовать для повторной отправки без загрузки файла.
    """
    __tablename__ = "media_file_cache"

    id = Column(Integer, primary_key=True, index=True)
    file_path = Column(String, unique=True, nullable=False, index=True)  # Путь к файлу, например "media/gifs/01_consent.gif"
    file_id = Column(String, nullable=False)  # file_id от Telegram
    file_type = Column(String, nullable=False)  # "animation" или "photo"
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)


async def log_button_click(
    session: AsyncSession,
    user_id: int | None,
    button_name: str,
) -> None:
    """
    Логирует нажатие на кнопку в боте.
    """
    event = ButtonEvent(
        user_id=user_id,
        button_name=button_name,
        created_at=datetime.utcnow(),
    )
    session.add(event)
    await session.commit()
    logger.debug(
        "Button click logged: user_id=%s button=%s",
        user_id,
        button_name,
    )


async def get_media_file_id(
    session: AsyncSession,
    file_path: str,
) -> tuple[str, str] | None:
    """
    Получает file_id и file_type из кэша для указанного файла.

    Returns:
        (file_id, file_type) если найден в кэше, иначе None
    """
    result = await session.execute(
        select(MediaFileCache)
        .where(MediaFileCache.file_path == file_path)
    )
    cached = result.scalar_one_or_none()
    if not cached:
        return None
    # file_type колонка not null, но оставляем защиту
    return (cached.file_id, cached.file_type or "animation")


async def save_media_file_id(
    session: AsyncSession,
    file_path: str,
    file_id: str,
    file_type: str,
) -> None:
    """
    Сохраняет file_id в кэш для указанного файла.
    Если запись уже существует, обновляет её.
    """
    result = await session.execute(
        select(MediaFileCache)
        .where(MediaFileCache.file_path == file_path)
    )
    cached = result.scalar_one_or_none()
    
    if cached:
        cached.file_id = file_id
        cached.file_type = file_type
        cached.updated_at = datetime.utcnow()
    else:
        cached = MediaFileCache(
            file_path=file_path,
            file_id=file_id,
            file_type=file_type,
        )
        session.add(cached)
    
    await session.commit()


# =========================
#   YCLIENTS SYNC
# =========================

async def sync_bonus_from_yclients(session: AsyncSession, user: "User") -> Optional[int]:
    """
    Синхронизирует баланс бонусов ИЗ YClients в бота.
    
    Получает актуальный баланс карты лояльности и обновляет БД бота.
    Вызывать при каждом показе баланса пользователю.
    
    Returns:
        Актуальный баланс или None если ошибка/нет карты
    """
    if not user.phone:
        return None
    
    try:
        try:
            from yclients_client import YClientsClient
        except ImportError:
            from bot.yclients_client import YClientsClient
        
        yclients = YClientsClient()
        yclients_balance = await yclients.get_bot_card_balance(user.phone)
        
        if yclients_balance is None:
            # Карты нет в YClients — ничего не делаем
            return None
        
        # Получаем бонус пользователя в БД
        bonus = await get_user_bonus_by_user_id(session, user.id)
        if not bonus:
            return yclients_balance
        
        # Если баланс отличается — обновляем в БД бота и пишем транзакцию
        if bonus.balance != yclients_balance:
            old_balance = bonus.balance
            delta = yclients_balance - bonus.balance

            # История операции (чтобы было видно в «Истории бонусов»)
            tx = BonusTransaction(
                user_id=user.id,
                amount=delta,
                type=BonusTransactionType.ACCRUAL if delta > 0 else BonusTransactionType.DEBIT,
                source=BonusTransactionSource.MANUAL,  # системная синхронизация, чтобы избежать миграции enum
                created_by="system",
                comment="Синхронизация YClients (оплата/списание)" if delta < 0 else "Синхронизация YClients (начисление)",
            )
            session.add(tx)

            bonus.balance = yclients_balance
            await session.commit()
            await session.refresh(bonus)
            logger.info(
                "Balance synced FROM YClients: user_id=%s old=%s new=%s delta=%s",
                user.id, old_balance, yclients_balance, delta
            )
        
        return yclients_balance
        
    except Exception as e:
        logger.error(
            "[SYNC FROM YCLIENTS] exception: user_id=%s error=%s",
            user.id, str(e)
        )
        return None


async def sync_bonus_to_yclients(user: "User", balance: int, delta: int = None) -> bool:
    """
    Синхронизирует баланс бонусов пользователя в YClients через карту лояльности.
    
    Вызывается после каждого изменения баланса.
    Безопасно обрабатывает ошибки — не ломает бота если YClients недоступен.
    
    Args:
        user: Пользователь с yclients_client_id и phone
        balance: Новый баланс бонусов
        delta: Изменение баланса (если известно) — для точной транзакции
        
    Returns:
        True если синхронизация успешна, False иначе
    """
    if not user.yclients_client_id:
        return False
    
    if not user.phone:
        return False
    
    try:
        # Пробуем оба варианта импорта (для бота и для админки)
        try:
            from yclients_client import YClientsClient
        except ImportError:
            from bot.yclients_client import YClientsClient
        
        yclients = YClientsClient()
        success = await yclients.sync_client_bonus_balance(
            client_id=user.yclients_client_id,
            balance=int(balance),
            phone=user.phone,
            delta=delta
        )
        return success
    except Exception as e:
        logger.error(
            "[YCLIENTS SYNC] exception: user_id=%s error=%s",
            user.id, str(e)
        )
        return False


