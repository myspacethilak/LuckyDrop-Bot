import os
import logging
import re
from datetime import datetime

logger = logging.getLogger(__name__)

# FIX: New, robust Markdown V1 escaping helper
def escape_markdown_V1(text: str) -> str:
    """Escapes characters that have special meaning in MarkdownV1."""
    if text is None:
        return 'N/A'

    # This regex is designed to escape all special characters that have a meaning in MarkdownV1.
    special_chars = r"_*[]()`>#+-=|{}.!"
    return re.sub(f"([{re.escape(special_chars)}])", r"\\\1", str(text))

# The escape_markdown_v2 function remains from before, but the MarkdownV1 version is used in admin_commands.py
def escape_markdown_v2(text: str) -> str:
    """Escapes characters that have special meaning in MarkdownV2.
    This is for raw text that should not be interpreted as Markdown at all."""
    if text is None:
        return 'N/A'

    # This regex is specifically designed to escape all characters that have special meaning in MarkdownV2.
    special_chars = r'_*[]()~`>#+-=|{}.!'
    return re.sub(f"([{re.escape(special_chars)}])", r"\\\1", str(text))