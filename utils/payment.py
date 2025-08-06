import os
import httpx
import logging
from datetime import datetime

from db.db_access import update_user_balance, add_recharge_to_history, get_user
from bot_config import UTC_TIMEZONE, CASHFREE_APP_ID, CASHFREE_SECRET_KEY

logger = logging.getLogger(__name__)

async def cashfree_webhook_handler(db, bot, payload: dict) -> str:
    """
    Handles incoming Cashfree Payouts webhook notifications.
    This function is now intentionally disabled.
    """
    logger.warning("Cashfree webhook handler is disabled. All payments are to be verified manually.")
    return "Webhook is disabled", 200