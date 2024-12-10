"""
Python app to test relevant python-dateutil functionality. Some code or partial code
was generated from ChatGPT and CodePilot
"""
from dateutil import parser, relativedelta, tz
from dateutil.relativedelta import relativedelta
from dateutil.rrule import rrule, rruleset, rrulestr, DAILY, MONTHLY
from datetime import datetime, timedelta

def run_main():
    print("\n=== Python Dateutil Functional API Examples without SparkSession ===\n")

    # 1. Parsing Dates and Times
    print("1. Parsing Dates and Times")
    date_str = "2024-11-22T15:30:00+05:30"
    parsed_date = parser.parse(date_str)
    print(f"Parsed date: {parsed_date}")

    # 2. Working with Time Zones
    print("\n2. Time Zone Handling")
    local_tz = tz.gettz("America/New_York")
    utc_tz = tz.UTC
    localized_date = parsed_date.astimezone(local_tz)
    print(f"Localized date (New York): {localized_date}")
    print(f"UTC date: {parsed_date.astimezone(utc_tz)}")

    # 3. Relative Delta
    print("\n3. Relative Delta for Date Arithmetic")
    today = datetime.now()
    one_year_later = today + relativedelta(years=1)
    two_months_earlier = today - relativedelta(months=2)
    print(f"Today: {today}")
    print(f"One year later: {one_year_later}")
    print(f"Two months earlier: {two_months_earlier}")

    # 4. Recurrence Rules (rrule)
    print("\n4. Recurrence Rules (rrule)")
    start_date = datetime(2024, 1, 1)
    rule = rrule(DAILY, count=5, dtstart=start_date)
    print(f"First 5 daily occurrences starting {start_date}:")
    for date in rule:
        print(date)

    # 5. Custom Recurrence Rules (rruleset)
    print("\n5. Custom Recurrence Rules (rruleset)")
    rset = rruleset()
    rset.rrule(rrule(MONTHLY, count=3, dtstart=start_date))
    rset.exdate(datetime(2024, 2, 1))  # Exclude February 1, 2024
    print("Custom recurrence with February excluded:")
    for date in rset:
        print(date)

    # 6. Recurrence Rules (rrulestr)
    print("\n6. Parsing Recurrence Rules (rrulestr)")
    # rrule_str = "RRULE:FREQ=DAILY;COUNT=3;DTSTART=20240101T000000Z"
    rrule_str =  "DTSTART:19970902T090000 RRULE:FREQ=DAILY;INTERVAL=10;COUNT=5"
    parsed_rrule = list(rrulestr(rrule_str))
    print("Dates from recurrence rule:")
    for date in parsed_rrule:
        print(date)

    # 7. Timedelta Arithmetic
    print("\n7. Timedelta Arithmetic")
    delta = timedelta(days=10)
    new_date = today + delta
    print(f"10 days from today: {new_date}")

    # 8. Combining Dateutil Features
    print("\n8. Combining Dateutil Features")
    complex_date = parser.parse("2024-11-22 15:30:00", tzinfos={"EST": -5 * 3600})
    print(f"Complex parsed date with custom timezone: {complex_date}")

if __name__ == "__main__":
    run_main()