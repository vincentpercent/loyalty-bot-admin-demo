from __future__ import annotations

from datetime import date, timedelta, datetime, timezone
from typing import Optional, List, Dict
import logging

import aiohttp

from config import settings


logger = logging.getLogger(__name__)

# ID типа карты "Бонусы бота" в YClients
BOT_LOYALTY_CARD_TYPE_ID = 100956


class YClientsClient:
    def __init__(self) -> None:
        self.base_url = settings.yclients.api_base_url
        self.partner_token = settings.yclients.partner_token
        self.user_token = settings.yclients.user_token
        self.company_id = settings.yclients.company_id

    @property
    def _headers(self) -> dict:
        return {
            "Content-Type": "application/json",
            "Accept": "application/vnd.yclients.v2+json",
            "Authorization": f"Bearer {self.partner_token}, User {self.user_token}",
        }

    async def _get(self, path: str, params: dict | None = None) -> dict | None:
        url = f"{self.base_url}/{path.lstrip('/')}"
        async with aiohttp.ClientSession(headers=self._headers) as session:
            async with session.get(url, params=params) as resp:
                text = await resp.text()
                if resp.status != 200:
                    logger.warning("[YCLIENTS GET] %s status=%s body=%s", url, resp.status, text)
                    return None
                try:
                    data = await resp.json()
                except Exception:
                    logger.warning("[YCLIENTS GET] invalid json from %s: %r", url, text)
                    return None
                return data

    async def _post(self, path: str, json: dict) -> dict | None:
        """
        Базовый POST-запрос к YCLIENTS.
        Используем для создания клиента.
        """
        url = f"{self.base_url}/{path.lstrip('/')}"
        async with aiohttp.ClientSession(headers=self._headers) as session:
            async with session.post(url, json=json) as resp:
                text = await resp.text()
                if resp.status not in (200, 201):
                    logger.warning("[YCLIENTS POST] %s status=%s body=%s", url, resp.status, text)
                    return None
                try:
                    data = await resp.json()
                except Exception:
                    logger.warning("[YCLIENTS POST] invalid json from %s: %r", url, text)
                    return None
                return data

    async def _delete(self, path: str) -> dict | None:
        url = f"{self.base_url}/{path.lstrip('/')}"
        async with aiohttp.ClientSession(headers=self._headers) as session:
            async with session.delete(url) as resp:
                text = await resp.text()
                if resp.status not in (200, 204):
                    logger.warning("[YCLIENTS DELETE] %s status=%s body=%s", url, resp.status, text)
                    return None
                try:
                    if text:
                        data = await resp.json()
                    else:
                        data = {"success": True, "data": None}
                except Exception:
                    data = {"success": True, "data": None}
                return data

    async def _put(self, path: str, json: dict) -> dict | None:
        """
        Базовый PUT-запрос к YCLIENTS.
        Используем для обновления клиента.
        """
        url = f"{self.base_url}/{path.lstrip('/')}"
        async with aiohttp.ClientSession(headers=self._headers) as session:
            async with session.put(url, json=json) as resp:
                text = await resp.text()
                if resp.status not in (200, 201):
                    logger.warning("[YCLIENTS PUT] %s status=%s body=%s", url, resp.status, text)
                    return None
                try:
                    data = await resp.json()
                except Exception:
                    logger.warning("[YCLIENTS PUT] invalid json from %s: %r", url, text)
                    return None
                return data

    async def find_client_by_phone(self, phone: str) -> tuple[Optional[dict], bool]:
        """
        Поиск клиента по телефону.
        GET /clients/{company_id}?phone=79991234567

        Возвращает (client_dict | None, is_error: bool):
        - client is None, is_error=False  -> клиента нет в YCLIENTS
        - client is dict, is_error=False  -> клиент найден
        - client is None, is_error=True   -> ошибка при обращении к API (нельзя решать, новый он или нет)
        """
        path = f"clients/{self.company_id}"
        params = {
            "phone": phone,
            "page": 1,
            "count": 20,
        }

        data = await self._get(path, params=params)
        if not isinstance(data, dict):
            logger.warning("[YCLIENTS find_client_by_phone] invalid data for phone %s: %r", phone, data)
            return None, True

        clients = data.get("data") or []
        if not isinstance(clients, list):
            logger.warning("[YCLIENTS find_client_by_phone] unexpected data format for phone %s: %r", phone, data)
            return None, True

        if not clients:
            # Клиентов с таким телефоном нет, но это НЕ ошибка
            logger.info("[YCLIENTS find_client_by_phone] no client for phone=%s", phone)
            return None, False

        logger.info("[YCLIENTS find_client_by_phone] client found for phone=%s id=%s", phone, clients[0].get("id"))
        return clients[0], False

    async def create_client(self, phone: str, name: str | None = None) -> tuple[Optional[dict], bool]:
        """
        Создание клиента в YCLIENTS, если его ещё нет.
        POST /clients/{company_id}

        Возвращает (client_dict | None, is_error: bool).
        - client is None, is_error=True   -> не удалось создать клиента
        """
        path = f"clients/{self.company_id}"
        payload: dict = {
            "phone": phone,
        }
        if name:
            # В разных инсталляциях может использоваться name/full_name, но телефон — ключевой
            payload["name"] = name

        data = await self._post(path, json=payload)
        if not isinstance(data, dict):
            logger.warning("[YCLIENTS create_client] invalid data for phone %s: %r", phone, data)
            return None, True

        # Ожидаем структуру вида {"success": true, "data": {...}, "meta": {...}}
        if not data.get("success"):
            logger.warning("[YCLIENTS create_client] success=False for phone %s body=%r", phone, data)
            return None, True

        client = data.get("data") or None
        if client:
            logger.info("[YCLIENTS create_client] created client id=%s for phone=%s", client.get("id"), phone)
        return client, False

    async def get_upcoming_records(self, client_id: int, days_ahead: int = 30) -> List[Dict]:
        """
        Получить будущие записи клиента на ближайшие days_ahead дней.
        Использует:
          GET /records/{company_id}?client_id=...&start_date=YYYY-MM-DD&end_date=YYYY-MM-DD
        """
        today = date.today()
        end = today + timedelta(days=days_ahead)

        params = {
            "client_id": client_id,
            "start_date": today.strftime("%Y-%m-%d"),
            "end_date": end.strftime("%Y-%m-%d"),
            "page": 1,
            "count": 100,
        }

        path = f"records/{self.company_id}"
        data = await self._get(path, params=params)
        if not isinstance(data, dict):
            logger.warning("[YCLIENTS get_upcoming_records] invalid data for client_id=%s: %r", client_id, data)
            return []

        records = data.get("data") or []
        if not isinstance(records, list):
            logger.warning("[YCLIENTS get_upcoming_records] unexpected data format for client_id=%s: %r", client_id, data)
            return []

        logger.info("[YCLIENTS get_upcoming_records] client_id=%s records_count=%s", client_id, len(records))
        return records

    async def get_all_records(
        self,
        start_date: date | None = None,
        end_date: date | None = None,
        page: int = 1,
        count: int = 100,
        include_deleted: bool = False,
    ) -> List[Dict]:
        """
        Получить все записи компании за указанный период.
        GET /records/{company_id}?start_date=YYYY-MM-DD&end_date=YYYY-MM-DD

        По умолчанию: от сегодня до +30 дней.
        """
        if start_date is None:
            start_date = date.today()
        if end_date is None:
            end_date = start_date + timedelta(days=30)

        params = {
            "start_date": start_date.strftime("%Y-%m-%d"),
            "end_date": end_date.strftime("%Y-%m-%d"),
            "page": page,
            "count": count,
        }
        if include_deleted:
            params["include_deleted"] = 1
            params["show_deleted"] = 1

        path = f"records/{self.company_id}"
        data = await self._get(path, params=params)
        if not isinstance(data, dict):
            logger.warning("[YCLIENTS get_all_records] invalid data: %r", data)
            return []

        records = data.get("data") or []
        if not isinstance(records, list):
            logger.warning("[YCLIENTS get_all_records] unexpected data format: %r", data)
            return []

        logger.info(
            "[YCLIENTS get_all_records] start=%s end=%s records_count=%s",
            start_date,
            end_date,
            len(records),
        )
        return records

    async def get_record_by_id(self, record_id: int) -> Optional[dict]:
        """
        Получить конкретную запись:
          GET /record/{company_id}/{record_id}

        Возвращает либо dict с записью, либо None при ошибке.
        """
        path = f"record/{self.company_id}/{record_id}"
        data = await self._get(path)
        if not isinstance(data, dict):
            logger.warning("[YCLIENTS get_record_by_id] invalid data for record_id=%s: %r", record_id, data)
            return None

        # Часто формат {"success": true, "data": {...}}
        record = data.get("data")
        if record is None:
            # fallback: вдруг сам объект — это уже запись
            if data.get("success") is None:
                record = data
            else:
                logger.warning("[YCLIENTS get_record_by_id] no data field for record_id=%s: %r", record_id, data)
                return None

        logger.info("[YCLIENTS get_record_by_id] got record_id=%s", record_id)
        return record

    async def delete_record(self, record_id: int) -> bool:
        """
        Удалить запись:
          DELETE /record/{company_id}/{record_id}
        В v2 ответ обычно содержит success/data/meta.
        """
        path = f"record/{self.company_id}/{record_id}"
        data = await self._delete(path)
        if not isinstance(data, dict):
            logger.warning("[YCLIENTS delete_record] invalid data for record_id=%s: %r", record_id, data)
            return False

        success = data.get("success")
        logger.info("[YCLIENTS delete_record] record_id=%s success=%s", record_id, success)
        return bool(success)

    async def has_completed_visit(self, client_id: int, days_back: int = 60) -> Optional[datetime]:
        """
        Возвращает дату ПОСЛЕДНЕГО завершённого/оплаченного визита за последние days_back дней,
        либо None, если таких визитов нет.

        Используется для реферальной логики:
        - факт визита
        - сравнение даты визита с моментом привязки реферального кода.
        """
        today = date.today()
        start = today - timedelta(days=days_back)

        params = {
            "client_id": client_id,
            "start_date": start.strftime("%Y-%m-%d"),
            "end_date": today.strftime("%Y-%m-%d"),
            "page": 1,
            "count": 100,
        }

        path = f"records/{self.company_id}"
        data = await self._get(path, params=params)
        if not isinstance(data, dict):
            logger.warning("[YCLIENTS has_completed_visit] invalid data for client_id=%s: %r", client_id, data)
            return None

        records = data.get("data") or []
        if not isinstance(records, list):
            logger.warning("[YCLIENTS has_completed_visit] unexpected data format for client_id=%s: %r", client_id, data)
            return None

        def normalize_dt(raw: str | None) -> Optional[datetime]:
            if not raw:
                return None
            try:
                dt = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
            except Exception:
                return None
            # Приводим к наивному UTC (без tzinfo), чтобы сопоставлять с datetime.utcnow()
            if dt.tzinfo is not None:
                dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
            return dt

        now = datetime.utcnow()
        latest_visit: Optional[datetime] = None

        for record in records:
            dt_raw = record.get("datetime") or record.get("date")
            dt_obj = normalize_dt(dt_raw)

            # Если дата в будущем — это запись, которая ещё не произошла
            if dt_obj and dt_obj > now:
                continue

            status = (
                record.get("attendance")
                or record.get("visit_attendance")
                or record.get("status")
                or ""
            )
            status_str = str(status).lower()

            # типичные флаги оплаты
            is_paid = bool(
                record.get("is_payed")
                or record.get("is_paid")
                or (str(record.get("payment_status", "")).lower() == "paid")
            )

            # эвристика "успешного визита"
            if (
                "visit" in status_str
                or "visited" in status_str
                or "completed" in status_str
                or is_paid
            ):
                if dt_obj is None:
                    continue
                if latest_visit is None or dt_obj > latest_visit:
                    latest_visit = dt_obj

        if latest_visit:
            logger.info("[YCLIENTS has_completed_visit] client_id=%s latest_visit=%s", client_id, latest_visit)
        else:
            logger.info("[YCLIENTS has_completed_visit] client_id=%s no completed visits found", client_id)

        return latest_visit

    async def get_client_loyalty_cards(self, phone: str) -> List[Dict]:
        """
        Получить карты лояльности клиента по телефону.
        GET /loyalty/cards/{phone}/0/{company_id}
        
        group_id=0 означает поиск по всей сети.
        """
        # Нормализуем телефон (убираем +, пробелы, дефисы)
        clean_phone = ''.join(c for c in phone if c.isdigit())
        
        path = f"loyalty/cards/{clean_phone}/0/{self.company_id}"
        data = await self._get(path)
        
        if not isinstance(data, dict):
            logger.warning("[YCLIENTS get_client_loyalty_cards] invalid data for phone=%s: %r", phone, data)
            return []
        
        cards = data.get("data") or []
        if not isinstance(cards, list):
            logger.info("[YCLIENTS get_client_loyalty_cards] no cards for phone=%s", phone)
            return []
        
        logger.info("[YCLIENTS get_client_loyalty_cards] phone=%s found %d cards", phone, len(cards))
        return cards

    async def issue_loyalty_card(self, phone: str, type_id: int = BOT_LOYALTY_CARD_TYPE_ID) -> Optional[Dict]:
        """
        Выдать карту лояльности клиенту по номеру телефона.
        POST /loyalty/cards/{company_id}
        
        Возвращает данные карты или None при ошибке.
        """
        # Нормализуем телефон (только цифры)
        clean_phone = ''.join(c for c in str(phone) if c.isdigit())
        
        path = f"loyalty/cards/{self.company_id}"
        payload = {
            "loyalty_card_type_id": type_id,
            "phone": int(clean_phone)
        }
        
        result = await self._post(path, json=payload)
        
        if result and result.get("success"):
            card = result.get("data", {})
            logger.info(
                "[YCLIENTS issue_loyalty_card] SUCCESS: phone=%s card_id=%s type_id=%s",
                phone, card.get("id"), type_id
            )
            return card
        else:
            logger.warning(
                "[YCLIENTS issue_loyalty_card] FAILED: phone=%s type_id=%s result=%r",
                phone, type_id, result
            )
            return None

    async def loyalty_transaction(self, card_id: int, amount: int, title: str = "Бонусы бота") -> Optional[Dict]:
        """
        Начислить или списать бонусы на карту лояльности.
        POST /company/{company_id}/loyalty/cards/{card_id}/manual_transaction
        
        amount > 0 — начисление
        amount < 0 — списание
        
        Возвращает обновлённые данные карты или None при ошибке.
        """
        path = f"company/{self.company_id}/loyalty/cards/{card_id}/manual_transaction"
        payload = {
            "amount": amount,
            "title": title
        }
        
        result = await self._post(path, json=payload)
        
        if result and result.get("success"):
            card = result.get("data", {})
            logger.info(
                "[YCLIENTS loyalty_transaction] SUCCESS: card_id=%s amount=%s new_balance=%s",
                card_id, amount, card.get("balance")
            )
            return card
        else:
            logger.warning(
                "[YCLIENTS loyalty_transaction] FAILED: card_id=%s amount=%s result=%r",
                card_id, amount, result
            )
            return None

    async def get_or_create_bot_loyalty_card(self, phone: str) -> Optional[int]:
        """
        Получить ID карты "Бонусы бота" для клиента.
        Если карты нет — создаёт её по номеру телефона.
        
        Возвращает card_id или None при ошибке.
        """
        # Ищем существующую карту нужного типа
        cards = await self.get_client_loyalty_cards(phone)
        
        for card in cards:
            if card.get("type_id") == BOT_LOYALTY_CARD_TYPE_ID:
                card_id = card.get("id")
                logger.info(
                    "[YCLIENTS get_or_create_bot_loyalty_card] found existing card: phone=%s card_id=%s",
                    phone, card_id
                )
                return card_id
        
        # Карты нет — создаём по телефону
        new_card = await self.issue_loyalty_card(phone, BOT_LOYALTY_CARD_TYPE_ID)
        if new_card:
            return new_card.get("id")
        
        return None

    async def sync_client_bonus_balance(self, client_id: int, balance: int, phone: str = None, delta: int = None) -> bool:
        """
        Синхронизирует баланс бонусов через карту лояльности YClients.
        
        Если передан delta — делает транзакцию на эту сумму.
        Если delta не передан — вычисляет разницу между текущим балансом карты и target balance.
        
        phone — телефон клиента (нужен для поиска/создания карты).
        
        Возвращает True если успешно, False если ошибка.
        """
        if not phone:
            # Нужно получить телефон клиента
            path = f"client/{self.company_id}/{client_id}"
            client_data = await self._get(path)
            if client_data and client_data.get("data"):
                phone = client_data["data"].get("phone")
            
            if not phone:
                logger.warning(
                    "[YCLIENTS sync_bonus] cannot get phone for client_id=%s",
                    client_id
                )
                return False
        
        # Получаем или создаём карту лояльности по телефону
        card_id = await self.get_or_create_bot_loyalty_card(phone)
        
        if not card_id:
            logger.warning(
                "[YCLIENTS sync_bonus] cannot get/create loyalty card: phone=%s",
                phone
            )
            return False
        
        # Если передан delta — просто делаем транзакцию
        if delta is not None and delta != 0:
            result = await self.loyalty_transaction(card_id, delta, "Бонусы бота")
            return result is not None
        
        # Иначе синхронизируем к целевому балансу
        # Получаем текущий баланс карты
        cards = await self.get_client_loyalty_cards(phone)
        current_balance = 0
        for card in cards:
            if card.get("id") == card_id:
                current_balance = card.get("balance", 0)
                break
        
        diff = balance - current_balance
        if diff == 0:
            logger.info(
                "[YCLIENTS sync_bonus] balance already synced: phone=%s balance=%s",
                phone, balance
            )
            return True
        
        result = await self.loyalty_transaction(card_id, diff, "Синхронизация бонусов")
        return result is not None

    async def get_bot_card_balance(self, phone: str) -> Optional[int]:
        """
        Получить баланс карты "Бонусы бота" клиента.
        
        Возвращает баланс или None если карты нет.
        """
        cards = await self.get_client_loyalty_cards(phone)
        
        for card in cards:
            if card.get("type_id") == BOT_LOYALTY_CARD_TYPE_ID:
                balance = card.get("balance", 0)
                logger.info(
                    "[YCLIENTS get_bot_card_balance] phone=%s balance=%s",
                    phone, balance
                )
                return int(balance)
        
        return None

    async def get_loyalty_card_types(self) -> List[Dict]:
        """
        Получить типы карт лояльности в салоне.
        GET /loyalty/card_types/salon/{company_id}
        """
        path = f"loyalty/card_types/salon/{self.company_id}"
        data = await self._get(path)
        
        if not isinstance(data, dict):
            logger.warning("[YCLIENTS get_loyalty_card_types] invalid data: %r", data)
            return []
        
        card_types = data.get("data") or []
        if not isinstance(card_types, list):
            # Может быть пустой ответ или ошибка
            logger.info("[YCLIENTS get_loyalty_card_types] no card types or error: %r", data)
            return []
        
        logger.info("[YCLIENTS get_loyalty_card_types] found %d card types", len(card_types))
        return card_types

