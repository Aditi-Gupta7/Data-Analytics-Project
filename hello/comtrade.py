from hello.api import API
from hello.un import UN
from hello.supervisorcomtrade import SupervisorComtrade
from hello.config import Config
import time


class Comtrade:
    def __init__(self):
        self.config = Config()
        self.un = UN()
        self.db = self.config.db

    def add_comtrade_data(self):
        print("\nIn data table")
        for year in self.config.env.years:
            for month in self.config.months:
                for country in self.config.env.countries:
                    try:
                        sc = SupervisorComtrade(country, year, month)
                        print("Checking for record: rtCode:", country, "ps:", year + month)
                        total_records, sup_response = sc.get_response_and_totalrecord(country, year, month)
                        if self.db.is_response_ok(sup_response) and total_records > 0:  # CHECK
                            for country2 in self.config.env.countries:
                                if country2 != country:
                                    print("ptCode: " + country2)
                                    for flow in self.config.flow:
                                        if not self.db.is_exist_in_trade_data(country, country2, year, month, flow):
                                            time.sleep(self.config.env.wait)
                                            df, response = self.un.get_trade_data(country, country2, flow, year, month)
                                            self.process_data_api_response(response, df, self.db, country, country2,
                                                                           year, month, sc)
                                        else:
                                            sc.is_atleast_one_exist = True
                            sc.report_is_complete()
                        # print("\n")
                    except Exception as err:
                        print(f'Error occurred 2: {err}')

    def process_data_api_response(self, response, dataframe, database, country1, country2, year, month, sc):
        api = API()
        is_response_error = api.is_response_error(response)
        if not is_response_error:
            record_added = self.process_data_api_success_response(dataframe, database, country1, year, month, sc)
            if record_added:
                sc.record_added = True
        else:
            sc.is_call_trade_data_api_error = True
            print("Failed to call API for rtCode:", country1, ", ps:", year + month, "ptCode:", country2)

    def process_data_api_success_response(self, dataframe, database, country, year, month, sc):
        record_added = False
        if not dataframe.empty:
            try:
                database.write_data(dataframe, self.config.env.comtrade_data_table, 'append', country, year, month)
                record_added = True
            except:
                sc.is_call_trade_data_api_error = True
        return record_added


