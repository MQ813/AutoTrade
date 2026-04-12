"""Generated KRX regular-session holiday dates.

Regenerate with:
`python tools/krx_holidays/convert_holidays.py`

Sources:
# - tools/krx_holidays/raw/krx_holidays_2024.xls
# - tools/krx_holidays/raw/krx_holidays_2025.xls
# - tools/krx_holidays/raw/krx_holidays_2026.xls
# - tools/krx_holidays/raw/krx_holidays_2027.xls
"""

from __future__ import annotations

from datetime import date

KRX_HOLIDAY_DATES = frozenset(
    {
        date(2024, 1, 1),
        date(2024, 2, 9),
        date(2024, 2, 12),
        date(2024, 3, 1),
        date(2024, 4, 10),
        date(2024, 5, 1),
        date(2024, 5, 6),
        date(2024, 5, 15),
        date(2024, 6, 6),
        date(2024, 8, 15),
        date(2024, 9, 16),
        date(2024, 9, 17),
        date(2024, 9, 18),
        date(2024, 10, 1),
        date(2024, 10, 3),
        date(2024, 10, 9),
        date(2024, 12, 25),
        date(2024, 12, 31),
        date(2025, 1, 1),
        date(2025, 1, 27),
        date(2025, 1, 28),
        date(2025, 1, 29),
        date(2025, 1, 30),
        date(2025, 3, 3),
        date(2025, 5, 1),
        date(2025, 5, 5),
        date(2025, 5, 6),
        date(2025, 6, 3),
        date(2025, 6, 6),
        date(2025, 8, 15),
        date(2025, 10, 3),
        date(2025, 10, 6),
        date(2025, 10, 7),
        date(2025, 10, 8),
        date(2025, 10, 9),
        date(2025, 12, 25),
        date(2025, 12, 31),
        date(2026, 1, 1),
        date(2026, 2, 16),
        date(2026, 2, 17),
        date(2026, 2, 18),
        date(2026, 3, 2),
        date(2026, 5, 1),
        date(2026, 5, 5),
        date(2026, 5, 25),
        date(2026, 8, 17),
        date(2026, 9, 24),
        date(2026, 9, 25),
        date(2026, 10, 5),
        date(2026, 10, 9),
        date(2026, 12, 25),
        date(2026, 12, 31),
        date(2027, 1, 1),
        date(2027, 2, 8),
        date(2027, 2, 9),
        date(2027, 3, 1),
        date(2027, 5, 5),
        date(2027, 5, 13),
        date(2027, 8, 16),
        date(2027, 9, 14),
        date(2027, 9, 15),
        date(2027, 9, 16),
        date(2027, 10, 4),
        date(2027, 10, 11),
        date(2027, 12, 27),
        date(2027, 12, 31),
    }
)
