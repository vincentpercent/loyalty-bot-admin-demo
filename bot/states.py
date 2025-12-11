from aiogram.fsm.state import State, StatesGroup


class Registration(StatesGroup):
    awaiting_consent = State()
    awaiting_phone = State()


class ReviewFlow(StatesGroup):
    """
    FSM для сценария "Бонус за отзыв":
    - awaiting_yandex_screenshot: ждём скриншот отзыва с Яндекс.Карт
    - awaiting_2gis_screenshot: ждём скриншот отзыва с 2ГИС
    """
    awaiting_yandex_screenshot = State()
    awaiting_2gis_screenshot = State()


class PromocodeFlow(StatesGroup):
    """
    FSM для ввода промокода.
    """
    awaiting_code = State()

