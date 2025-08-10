import asyncio
from motor.motor_asyncio import AsyncIOMotorClient
import os

# Assume bot_config.py is in the same directory
# If not, you might need to adjust the import path
from bot_config import MONGO_URI

# --- CHANGE THIS TO THE USER ID YOU WANT TO CLEAR ---
USER_ID_TO_CLEAR = 7922195865 

async def clear_user_data():
    """Connects to the database and deletes a user document."""
    if not MONGO_URI:
        print("Error: MONGO_URI is not set in bot_config.py. Cannot connect to database.")
        return

    db_client = AsyncIOMotorClient(MONGO_URI)
    db = db_client.lotterydb

    print(f"Connecting to database and deleting data for user ID: {USER_ID_TO_CLEAR}...")

    # Delete the user document
    result = await db.users.delete_one({"telegram_id": USER_ID_TO_CLEAR})

    if result.deleted_count > 0:
        print(f"✅ Successfully deleted user ID {USER_ID_TO_CLEAR} from the 'users' collection.")
    else:
        print(f"⚠️ User ID {USER_ID_TO_CLEAR} not found in the 'users' collection. No action needed.")

    db_client.close()
    print("Database connection closed.")

if __name__ == '__main__':
    asyncio.run(clear_user_data())