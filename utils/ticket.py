# utils/ticket.py:
import random
import string
import logging

from db.db_access import get_user_counts_by_referral_source # Corrected import based on db_access.py methods

logger = logging.getLogger(__name__)

async def generate_unique_ticket_code(db):
    """Generates a unique 6-digit alphanumeric ticket code."""
    while True:
        code = ''.join(random.choices(string.digits, k=6))

        user_with_code = await db.users.find_one({"last_ticket_code": code})
        if user_with_code:
            logger.debug(f"Generated ticket code {code} already in use by a user. Retrying.")
            continue

        pot_with_code = await db.pots.find_one({
            "status": {"$in": ["open", "closed", "revealed"]},
            "participants.ticket_code": code
        })
        if pot_with_code:
            logger.debug(f"Generated ticket code {code} already in use in a pot. Retrying.")
            continue

        logger.info(f"Generated unique ticket code: {code}")
        return code