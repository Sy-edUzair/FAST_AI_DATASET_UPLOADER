from datetime import datetime


def calculate_priority(file_type, size_mb):
    score = 1

    if file_type in ("zip", "csv"):
        score += 2

    if size_mb > 50:
        score += 1

    hour = datetime.now().hour  # local server time

    if hour < 9 or hour >= 18:  # outside 9 to 6
        score += 1

    if score <= 2:
        level = "Low"
    elif score <= 4:
        level = "Medium"
    else:
        level = "High"

    return score, level
