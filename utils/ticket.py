import os
import random
import string
import logging
from io import BytesIO

from PIL import Image, ImageDraw, ImageFont

from aiogram.types import FSInputFile

from db.db_access import get_user
from utils.helpers import escape_markdown_v2

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

def generate_ticket_image(code: str, user_id: str, referral_name: str = None):
    base_path = "assets/ticket_template.png"
    output_path = f"generated_tickets/{user_id}_{code}.png"
    os.makedirs("generated_tickets", exist_ok=True)

    try:
        base = Image.open(base_path).convert("RGBA")
    except Exception as e:
        logger.error(f"Failed to load base image: {e}")
        return None

    draw = ImageDraw.Draw(base)

    try:
        font_code = ImageFont.truetype("assets/BebasNeue-Regular.ttf", 90)
        font_uid = ImageFont.truetype("assets/BebasNeue-Regular.ttf", 35)
    except Exception as e:
        logger.warning(f"Failed to load custom fonts: {e}. Using default.")
        font_code = ImageFont.load_default(size=90)
        font_uid = ImageFont.load_default(size=35)

    w, h = base.size

    text = code
    code_bbox = draw.textbbox((0, 0), text, font=font_code)
    code_w = code_bbox[2] - code_bbox[0]
    code_y = h * 0.42 
    code_x = (w - code_w) / 2
    draw.text((code_x, code_y), text, font=font_code, fill="black")

    uid_text_value = user_id
    uid_bbox = draw.textbbox((0, 0), uid_text_value, font=font_uid)
    uid_w = uid_bbox[2] - uid_bbox[0]
    uid_h = uid_bbox[3] - uid_bbox[1]

    uid_box_left = w * 0.795
    uid_box_top = h * 0.77
    uid_box_right = w * 0.98
    uid_box_bottom = h * 0.95

    uid_box_width = uid_box_right - uid_box_left
    uid_box_height = uid_box_bottom - uid_box_top

    uid_x = uid_box_left + (uid_box_width - uid_w) / 2
    uid_y = uid_box_top + (uid_box_height - uid_h) / 2

    draw.text((uid_x, uid_y), uid_text_value, font=font_uid, fill="white")

    base.save(output_path)
    return output_path