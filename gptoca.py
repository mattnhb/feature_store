import re

day_range = "dia_15_16"
start, end = map(int, re.findall(r"\d+", day_range))
numbers = set(range(start, end + 1))
print(numbers)  # Output: {1, 2, 3, 4, 5, 6, 7, 8}
