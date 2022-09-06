import time
import sys
from sqlalchemy import create_engine
from dotenv import load_dotenv
import django
from hello.comtrade import Comtrade
from hello.config import Config
from hello.un import UN

django.setup()
load_dotenv()

# Dev branch
class Main:
    def __init__(self):
        print("Starting the process - Dev")
        self.validate_wait()

        # Database related variables
        # self.data_range_table = os.getenv('data_range_table')
        self.db = create_engine(config.env.conn_string, pool_pre_ping=True)
                                #connect_args={"options": "-c statement_timeout=60000"})#

    def validate_wait(self):
        if int(config.env.wait < 0):
            print("Wait should not be a negative value. Please give a valid value.")
            sys.exit()

    def close_conn(self):
        try:
            self.db.dispose()
            print("DB connection closed successfully")
        except Exception as err:
            print(f'An error occurred: {err}')

    def supervisor(self):
        config = Config()
        un = UN()
        db = config.db
        print("In supervisor table")
        for year in config.env.years:
            for country in config.env.countries:
                for month in config.months:
                    try:
                        print("Checking for record: rtCode:", country, ", ps:", year + month)
                        is_exist = db.is_exist_in_supervisor(country, year, month)
                        if is_exist:
                            total_records, response = db.get_response_and_totalrecord_from_supervisor(country,
                                                                                                      year, month)
                            if response == '<Response [200]>' and total_records >= 0:
                                # print(type(response))
                                # print("Record exists WITHOUT ERROR")
                                pass
                            else:
                                time.sleep(config.env.wait)
                                df, response = un.get_trade_record_count(country, year, month)
                                if response == '<Response [200]>':
                                    db.delete_record_from_supervisor(country, year, month)
                                    db.write_data(df, config.env.supervisor_table, 'append', country, year, month)
                        else:
                            if not is_exist:
                                time.sleep(config.env.wait)
                                # print("Record does not exist")
                                df, _ = un.get_trade_record_count(country, year, month)
                                # print(df, "\n")
                                db.write_data(df, config.env.supervisor_table, 'append', country, year, month)
                                # print("\n")
                    except Exception as err:
                        print(f'Error occurred 1: {err}')

    def comtrade_data(self):
        comtrade = Comtrade()
        comtrade.add_comtrade_data()


config = Config()
main = Main()
main.supervisor()
main.comtrade_data()
main.close_conn()
