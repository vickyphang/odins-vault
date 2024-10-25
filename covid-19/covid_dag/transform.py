from numpy import abs as np_abs


class Transform:

    def __init__(self, data : pd.DataFrame) -> None:
        self.data = data

    def transform_data(self) -> pd.DataFrame:
        daily_data = self.data.copy()
        daily_data.drop(["location_iso_code", "new_cases", "new_deaths", "new_recovered", "new_active_cases", "province", "country", "continent", "island", "time_zone", "special_status", "total_regencies", "total_cities", "total_districts", "total_urban_villages", "total_rural_villages", "area", "new_cases_per_million", "total_cases_per_million", "new_deaths_per_million", "total_deaths_per_million", "case_fatality_rate", "case_recovered_rate", "growth_factor_of_new_cases", "growth_factor_of_new_deaths", "city_or_regency"],axis=1,inplace=True)
        daily_data.drop_duplicates(subset='location', keep="last")
        return daily_data