from aiogram.types import (
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    ReplyKeyboardMarkup,
    KeyboardButton,
)


def consent_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="✔️ Согласен", callback_data="consent_accept")]
        ]
    )


def phone_request_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        resize_keyboard=True,
        keyboard=[
            [
                KeyboardButton(
                    text="📲 Взять номер из Telegram",
                    request_contact=True,
                )
            ],
            [
                KeyboardButton(text="Отмена"),
            ],
        ],
    )


def main_menu_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        resize_keyboard=True,
        keyboard=[
            [
                KeyboardButton(text="💈 Запись"),
            ],
            [
                KeyboardButton(text="🎁 Бонусная программа"),
            ],
            [
                KeyboardButton(text="🆘 Помощь"),
                KeyboardButton(text="💰 Баланс"),
            ],
        ],
    )


def loyalty_menu_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        resize_keyboard=True,
        keyboard=[
            [KeyboardButton(text="🎉 Welcome bonus")],
            [KeyboardButton(text="📢 Бонус за подписку на канал")],
            [KeyboardButton(text="⭐ Бонус за отзыв")],
            [KeyboardButton(text="🎟 Ввести промокод")],
            [KeyboardButton(text="🤝 Реферальная программа")],
            [KeyboardButton(text="🔙 Назад в главное меню")],
        ],
    )

