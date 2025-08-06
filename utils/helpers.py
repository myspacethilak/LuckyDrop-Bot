import os
import logging
from datetime import datetime
import re

logger = logging.getLogger(__name__)

def escape_markdown_v2(text: str) -> str:
    """Escapes characters that have special meaning in MarkdownV2.
    Now handles None values gracefully."""
    if text is None:
        return 'N/A'
    special_chars = r'_*[]()~`>#+-=|{}.!'
    escaped_text = ""
    for char in text:
        if char in special_chars:
            escaped_text += '\\' + char
        else:
            escaped_text += char
    return escaped_text