import pandas as pd
import holidays

def model(dbt, session):
    dbt.config(
        materialized="table",
        packages=["pandas", "holidays"]
    )

    # US holidays, because the data is from NY.
    us_holidays = holidays.US()

    # Generate the date range
    dates = pd.date_range(start='2016-01-01', end='2018-12-31', freq='D')

    # Create the DataFrame
    df = pd.DataFrame({'DATE': dates})

    df['DATE'] = df['DATE'].dt.date  # Convert timestamp to date
    df["IS_HOLIDAY"] = df["DATE"].apply(lambda date: date in us_holidays)
    df["HOLIDAY_NAME"] = df["DATE"].apply(lambda date: us_holidays.get(date))

    return df