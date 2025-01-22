import pandas as pd
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import random

# Helper function to generate random dates
def random_date(start_date, end_date):
    delta = end_date - start_date
    random_days = random.randint(0, delta.days)
    return start_date + timedelta(days=random_days)

start_date = datetime(2000, 1, 1)
end_date = datetime(2030, 1, 1)
num_rows = 5
data = {
        "id": [i for i in range(1,  num_rows+1)],
        "start_date": [random_date(start_date, end_date) for _ in range(num_rows)],
        "end_date" : [random_date(start_date, end_date) for _ in range(num_rows)],
    }

df = pd.DataFrame(data)

# Add a new column with the days difference
df['days_difference'] = df.apply(lambda row: (row['end_date'] - row['start_date']).days, axis=1)

print(df)