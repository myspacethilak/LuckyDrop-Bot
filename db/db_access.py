import os
from datetime import datetime, timedelta
from motor.motor_asyncio import AsyncIOMotorClient
import logging
import pytz

from bot_config import MONGO_URI, UTC_TIMEZONE, IST_TIMEZONE

logger = logging.getLogger(__name__)

async def init_db(db):
    await db.users.create_index("telegram_id", unique=True)
    await db.users.create_index("referral_code", unique=True)
    await db.pots.create_index("date", unique=True)
    await db.tickets.create_index("code", unique=True)
    logger.info("MongoDB indexes created/ensured.")

async def get_user(db, telegram_id: int):
    return await db.users.find_one({"telegram_id": telegram_id})

async def create_user(db, telegram_id: int, username: str = None, referrer_id: int = None):
    user_data = {
        "telegram_id": telegram_id,
        "username": username,
        "real_balance": 0.0,
        "bonus_balance": 0.0,
        "referral_code": f"LUCKY{telegram_id}",
        "referred_by": referrer_id,
        "referral_count": 0,
        "joined_date": datetime.now(UTC_TIMEZONE),
        "last_ticket_date": None,
        "last_ticket_code": None,
        "referred_users_tickets": [],
        "recharge_history": [],
        "upi_id": None
    }
    await db.users.insert_one(user_data)
    logger.info(f"New user created: {telegram_id}")
    return user_data

async def update_user_balance(db, telegram_id: int, real_amount: float = 0.0, bonus_amount: float = 0.0):
    update_fields = {}
    if real_amount != 0:
        update_fields["real_balance"] = real_amount
    if bonus_amount != 0:
        update_fields["bonus_balance"] = bonus_amount

    if not update_fields:
        return None

    result = await db.users.find_one_and_update(
        {"telegram_id": telegram_id},
        {"$inc": update_fields},
        return_document=True
    )
    if result:
        logger.info(f"User {telegram_id} balance updated: real={result.get('real_balance')}, bonus={result.get('bonus_balance')}")
    return result

async def update_user_upi(db, telegram_id: int, upi_id: str):
    await db.users.update_one(
        {"telegram_id": telegram_id},
        {"$set": {"upi_id": upi_id}}
    )
    logger.info(f"User {telegram_id} UPI ID updated.")

async def add_recharge_to_history(db, telegram_id: int, amount: float, status: str, order_id: str, user_name: str = None):
    recharge_data = {
        "amount": amount,
        "status": status,
        "timestamp": datetime.now(UTC_TIMEZONE),
        "order_id": order_id,
        "user_name": user_name
    }

    await db.users.update_one(
        {"telegram_id": telegram_id},
        {"$push": {"recharge_history": recharge_data}}
    )
    logger.info(f"Recharge history updated for user {telegram_id}")

async def get_pot_by_date(db, date_str: str):
    return await db.pots.find_one({"date": date_str})

async def add_user_to_pot(db, pot_id, telegram_id: int, ticket_code: str):
    await db.pots.update_one(
        {"_id": pot_id},
        {"$push": {"participants": {"telegram_id": telegram_id, "ticket_code": ticket_code}},
         "$inc": {"total_tickets": 1}}
    )
    logger.info(f"User {telegram_id} added to pot {pot_id} with ticket {ticket_code}")

async def get_users_in_pot(db, pot_id):
    pot = await db.pots.find_one({"_id": pot_id})
    return pot['participants'] if pot else []

async def update_user_ticket(db, telegram_id: int, ticket_code: str):
    await db.users.update_one(
        {"telegram_id": telegram_id},
        {"$set": {"last_ticket_date": datetime.now(UTC_TIMEZONE), "last_ticket_code": ticket_code}}
    )
    logger.info(f"User {telegram_id} last ticket updated.")

async def update_pot_status(db, pot_id, status: str):
    await db.pots.update_one({"_id": pot_id}, {"$set": {"status": status}})
    logger.info(f"Pot {pot_id} status updated to {status}")

async def set_pot_winners(db, pot_id, winners: list):
    await db.pots.update_one({"_id": pot_id}, {"$set": {"winners": winners}})
    logger.info(f"Winners set for pot {pot_id}: {winners}")

async def update_pot_prize_pool(db, pot_id, prize_pool: float):
    await db.pots.update_one({"_id": pot_id}, {"$set": {"prize_pool": prize_pool}})
    logger.info(f"Prize pool set for pot {pot_id}: {prize_pool}")

async def get_all_users(db):
    return await db.users.find({}).to_list(length=None)

async def get_total_balance(db):
    pipeline = [
        {"$group": {
            "_id": None,
            "total_real": {"$sum": "$real_balance"},
            "total_bonus": {"$sum": "$bonus_balance"}
        }}
    ]
    result = await db.users.aggregate(pipeline).to_list(length=1)
    if result:
        return result[0]['total_real'], result[0]['total_bonus']
    return 0.0, 0.0

async def get_total_locked_funds(db):
    current_pot_data = await db.pots.find_one({"status": "open"})
    if current_pot_data:
        return current_pot_data['total_tickets'] * current_pot_data['ticket_price']
    return 0.0

async def get_user_counts_by_referral_source(db):
    pipeline = [
        {"$group": {
            "_id": "$referred_by",
            "count": {"$sum": 1}
        }},
        {"$sort": {"count": -1}}
    ]
    return await db.users.aggregate(pipeline).to_list(length=None)

async def get_all_pots(db):
    return await db.pots.find({}).to_list(length=None)

async def get_all_referrals(db):
    pipeline = [
        {"$match": {"referral_count": {"$gt": 0}}},
        {"$project": {
            "telegram_id": 1,
            "username": 1,
            "referral_code": 1,
            "referral_count": 1
        }}
    ]
    return await db.users.aggregate(pipeline).to_list(length=None)

async def mark_referred_user_ticket_bought(db, referrer_id: int, referred_user_id: int):
    await db.users.update_one(
        {"telegram_id": referrer_id},
        {"$addToSet": {"referred_users_tickets": referred_user_id}}
    )
    logger.info(f"Referrer {referrer_id} now registered that {referred_user_id} bought a ticket.")

async def check_referred_user_ticket_status(db, referrer_id: int, referred_user_id: int):
    user = await db.users.find_one(
        {"telegram_id": referrer_id, "referred_users_tickets": referred_user_id}
    )
    return bool(user)

async def increment_referral_count(db, telegram_id: int):
    await db.users.update_one(
        {"telegram_id": telegram_id},
        {"$inc": {"referral_count": 1}}
    )
    logger.info(f"Referral count incremented for user {telegram_id}")