from datetime import datetime

def parse_russian_datetime(date_str: str) -> datetime:
    """
    Parses a Russian date string and returns a datetime object with the specified year.

    Args:
        date_str (str): The date string in the format 'DD MMM HH:MM'.
        year (int): The year to set in the datetime object. Default is 2024.

    Returns:
        datetime: A datetime object with the specified year.
    """
    # Define a mapping for Russian month names to numbers
    month_mapping = {
        'янв': '01',
        'фев': '02',
        'мар': '03',
        'апр': '04',
        'май': '05',
        'июн': '06',
        'июл': '07',
        'авг': '08',
        'сен': '09',
        'окт': '10',
        'ноя': '11',
        'дек': '12'
    }

    # Split the string to replace the month name with a number
    day, month_name, time = date_str.split()
    month = month_mapping[month_name]

    # Create a new date string with the specified year
    date_with_year = f'{year}-{month}-{day} {time}'

    # Parse the date string into a datetime object

    return datetime.strptime(date_with_year, '%Y-%m-%d %H:%M')