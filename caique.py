from datetime import date, timedelta

def last_day_of_month(year, month):
    next_month = month % 12 + 1
    next_year = year + month // 12
    return date(next_year, next_month, 1) - timedelta(days=1)

def generate_last_two_years_dates():
    today = date.today()
    last_two_years = [today.year - 2, today.year - 1]

    return [last_day_of_month(year, month) for year in last_two_years for month in range(1, 13)]

if __name__ == "__main__":
    dates_list = generate_last_two_years_dates()
    for date_obj in dates_list:
        print(date_obj)
