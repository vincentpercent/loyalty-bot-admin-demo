#!/usr/bin/env python3
"""
Скрипт миграции бонусов из бота в YClients.

Для каждого пользователя с бонусами:
1. Создаёт карту "Бонусы бота" в YClients (если нет)
2. Начисляет разницу между балансом бота и балансом карты

Запуск:
  docker-compose exec bot python migrate_bonuses.py
  
  Или с параметром --dry-run для проверки без изменений:
  docker-compose exec bot python migrate_bonuses.py --dry-run
"""
import asyncio
import sys

sys.path.insert(0, '/app')

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from db import AsyncSessionLocal, User, UserBonus
from yclients_client import YClientsClient, BOT_LOYALTY_CARD_TYPE_ID


async def migrate_bonuses(dry_run: bool = False):
    """Миграция бонусов всех пользователей в YClients."""
    
    print("=" * 60)
    print("МИГРАЦИЯ БОНУСОВ В YCLIENTS")
    print("=" * 60)
    
    if dry_run:
        print("⚠️  РЕЖИМ ПРОВЕРКИ (--dry-run) — изменения НЕ будут применены\n")
    else:
        print("🚀 БОЕВОЙ РЕЖИМ — изменения будут применены\n")
    
    yclients = YClientsClient()
    
    stats = {
        "total": 0,
        "no_phone": 0,
        "no_yclients_id": 0,
        "zero_balance": 0,
        "already_synced": 0,
        "synced": 0,
        "card_created": 0,
        "errors": 0,
    }
    
    async with AsyncSessionLocal() as session:
        # Получаем всех пользователей с их бонусами
        result = await session.execute(
            select(User, UserBonus)
            .outerjoin(UserBonus, User.id == UserBonus.user_id)
        )
        rows = result.all()
        
        print(f"Найдено пользователей: {len(rows)}\n")
        print("-" * 60)
        
        for user, bonus in rows:
            stats["total"] += 1
            bot_balance = bonus.balance if bonus else 0
            
            # Пропускаем без телефона
            if not user.phone:
                print(f"❌ User {user.id}: нет телефона")
                stats["no_phone"] += 1
                continue
            
            # Пропускаем без YClients ID
            if not user.yclients_client_id:
                print(f"⚠️  User {user.id} ({user.phone}): нет yclients_client_id")
                stats["no_yclients_id"] += 1
                continue
            
            # Пропускаем с нулевым балансом
            if bot_balance == 0:
                stats["zero_balance"] += 1
                continue
            
            print(f"\n👤 User {user.id}: {user.full_name or user.username or 'N/A'}")
            print(f"   Телефон: {user.phone}")
            print(f"   Баланс в боте: {bot_balance}₽")
            
            try:
                # Получаем карты клиента
                cards = await yclients.get_client_loyalty_cards(user.phone)
                
                # Ищем карту "Бонусы бота"
                bot_card = None
                for card in cards:
                    if card.get("type_id") == BOT_LOYALTY_CARD_TYPE_ID:
                        bot_card = card
                        break
                
                if bot_card:
                    card_id = bot_card.get("id")
                    card_balance = bot_card.get("balance", 0)
                    print(f"   Карта YClients: ID {card_id}, баланс {card_balance}₽")
                else:
                    card_balance = 0
                    print(f"   Карта YClients: НЕТ (будет создана)")
                    
                    if not dry_run:
                        new_card = await yclients.issue_loyalty_card(user.phone, BOT_LOYALTY_CARD_TYPE_ID)
                        if new_card:
                            card_id = new_card.get("id")
                            stats["card_created"] += 1
                            print(f"   ✅ Карта создана: ID {card_id}")
                        else:
                            print(f"   ❌ Ошибка создания карты")
                            stats["errors"] += 1
                            continue
                    else:
                        stats["card_created"] += 1
                
                # Вычисляем разницу
                diff = bot_balance - card_balance
                
                if diff == 0:
                    print(f"   ✅ Уже синхронизировано")
                    stats["already_synced"] += 1
                    continue
                
                print(f"   Разница: {diff:+}₽")
                
                if not dry_run:
                    # Начисляем разницу
                    if bot_card:
                        result = await yclients.loyalty_transaction(
                            card_id, 
                            diff, 
                            "Миграция бонусов из бота"
                        )
                    else:
                        # Для новой карты делаем транзакцию
                        result = await yclients.loyalty_transaction(
                            card_id,
                            bot_balance,
                            "Миграция бонусов из бота"
                        )
                    
                    if result:
                        new_balance = result.get("balance", 0)
                        print(f"   ✅ Синхронизировано! Новый баланс: {new_balance}₽")
                        stats["synced"] += 1
                    else:
                        print(f"   ❌ Ошибка синхронизации")
                        stats["errors"] += 1
                else:
                    print(f"   [dry-run] Будет начислено: {diff:+}₽")
                    stats["synced"] += 1
                    
            except Exception as e:
                print(f"   ❌ Ошибка: {e}")
                stats["errors"] += 1
    
    # Итоги
    print("\n" + "=" * 60)
    print("ИТОГИ МИГРАЦИИ")
    print("=" * 60)
    print(f"Всего пользователей:     {stats['total']}")
    print(f"Без телефона:            {stats['no_phone']}")
    print(f"Без YClients ID:         {stats['no_yclients_id']}")
    print(f"С нулевым балансом:      {stats['zero_balance']}")
    print(f"Уже синхронизированы:    {stats['already_synced']}")
    print(f"Карт создано:            {stats['card_created']}")
    print(f"Синхронизировано:        {stats['synced']}")
    print(f"Ошибок:                  {stats['errors']}")
    print("=" * 60)
    
    if dry_run:
        print("\n⚠️  Это была проверка. Для применения запустите без --dry-run")


if __name__ == "__main__":
    dry_run = "--dry-run" in sys.argv
    asyncio.run(migrate_bonuses(dry_run))

