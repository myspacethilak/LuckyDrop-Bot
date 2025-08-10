import asyncio
from motor.motor_asyncio import AsyncIOMotorClient
import os

from bot_config import MONGO_URI

# --- SET THE IDs FOR YOUR TEST ACCOUNTS ---
REFERRER_ID = 8094551302
REFERRED_USER_ID = 7922195865

async def clear_test_data():
    """Connects to the database and clears all relevant test data."""
    if not MONGO_URI:
        print("Error: MONGO_URI is not set. Cannot connect.")
        return

    db_client = AsyncIOMotorClient(MONGO_URI)
    db = db_client.lotterydb

    print(f"Clearing test data for referrer {REFERRER_ID} and referred user {REFERRED_USER_ID}...")

    # 1. Delete the referred user's document
    result = await db.users.delete_one({"telegram_id": REFERRED_USER_ID})
    if result.deleted_count > 0:
        print(f"✅ Deleted user document for ID {REFERRED_USER_ID}.")
    else:
        print(f"⚠️ User document for ID {REFERRED_USER_ID} not found.")

    # 2. Remove the referred user's ID from the referrer's tracking list
    result = await db.users.update_one(
        {"telegram_id": REFERRER_ID},
        {"$pull": {"referred_users_tickets": REFERRED_USER_ID}}
    )
    if result.modified_count > 0:
        print(f"✅ Removed referred user ID {REFERRED_USER_ID} from referrer's tracking list.")
    else:
        print(f"⚠️ Referred user ID {REFERRED_USER_ID} was not in referrer's tracking list.")

    db_client.close()
    print("Database connection closed. Test data is now cleared for a fresh start.")

if __name__ == '__main__':
    asyncio.run(clear_test_data())