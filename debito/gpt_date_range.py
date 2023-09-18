from datetime import datetime, timedelta, date
from dateutil.relativedelta import relativedelta

current_date = date.today()

months_ago = 24
first_day_first_month = current_date.replace(day=1) - relativedelta(
    months=months_ago
)
last_day_last_month = current_date.replace(day=1) - timedelta(days=1)


print("First day of the first month:", first_day_first_month.strftime("%Y-%m-%d"))
print("Last day of the last month:", last_day_last_month.strftime("%Y-%m-%d"))

