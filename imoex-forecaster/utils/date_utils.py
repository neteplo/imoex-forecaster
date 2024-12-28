from datetime import datetime


def parse_russian_datetime(date_str) -> datetime:
    """
    Разбирает строку даты на русском языке и возвращает объект datetime.
    Если год присутствует в строке, используется этот год.
    В противном случае используется текущий год.

    Args:
        date_str (str): Строка даты в формате 'DD MMM YYYY HH:MM' или 'DD MMM HH:MM'.

    Returns:
        datetime: Объект datetime с соответствующим годом.
    """
    MONTH_MAPPINGS = {
        "янв": "01",
        "фев": "02",
        "мар": "03",
        "апр": "04",
        "май": "05",
        "июн": "06",
        "июл": "07",
        "авг": "08",
        "сен": "09",
        "окт": "10",
        "ноя": "11",
        "дек": "12",
    }

    parts = date_str.split()

    if len(parts) == 4:
        day, month_name, year, time = parts
    else:
        day, month_name, time = parts
        year = datetime.now().year

    month = MONTH_MAPPINGS[month_name]
    date_with_year = f"{year}-{month}-{day} {time}"
    parsed_date = datetime.strptime(date_with_year, "%Y-%m-%d %H:%M")

    return parsed_date
