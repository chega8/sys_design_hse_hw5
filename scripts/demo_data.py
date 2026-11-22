import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

COLUMNS = (
    "transaction_time", "merch", "cat_id", "amount", "name_1", "name_2",
    "gender", "street", "one_city", "us_state", "post_code", "lat", "lon",
    "population_city", "jobs", "merchant_lat", "merchant_lon", "target",
)
CITIES = (
    ("CA", "Los Angeles", 90001, 34.05, -118.24),
    ("NY", "New York", 10001, 40.71, -74.01),
    ("TX", "Austin", 73301, 30.27, -97.74),
    ("WA", "Seattle", 98101, 47.60, -122.33),
)
SHOPS = (
    ("North Market", "grocery"),
    ("Daily Foods", "grocery"),
    ("Metro Tech", "electronics"),
    ("Good Things", "electronics"),
    ("Open Road", "travel"),
    ("City Tickets", "travel"),
)
FRAUD_RATE = {"grocery": .02, "electronics": .05, "travel": .10}


def main():
    path = Path("data/train.csv")
    path.parent.mkdir(parents=True, exist_ok=True)
    rng = random.Random(42)
    with path.open("x", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(COLUMNS)
        for month in range(1, 7):
            start = datetime(2019, month, 1)
            for index in range(2400):
                state, city, zip_code, lat, lon = rng.choice(CITIES)
                store, category = rng.choice(SHOPS)
                fraud = int(rng.random() < FRAUD_RATE[category])
                amount = rng.uniform(350, 1600) if fraud else min(420, rng.lognormvariate(3.8, .8))
                timestamp = start + timedelta(minutes=index * 15)
                writer.writerow([
                    timestamp.strftime("%Y-%m-%d %H:%M"), store, category, round(amount, 2),
                    "Test", "Customer", rng.choice(("M", "F")), "Test Street",
                    city, state, zip_code, lat, lon, 100000, rng.choice(("writer", "teacher")),
                    round(lat + rng.uniform(-.4, .4), 6),
                    round(lon + rng.uniform(-.4, .4), 6), fraud,
                ])
    print(path)


if __name__ == "__main__":
    main()
