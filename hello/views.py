import time
import os
import sys

import sqlalchemy
from sqlalchemy import create_engine
from dotenv import load_dotenv
import django
from . import un
from . import db
# import un
# import db
django.setup()
load_dotenv()


class Comtrade:
    def __init__(self):
        print("Starting the process")
        # self.count = 0
        # Get variables from environment file
        self.flow = ['1', '2', '3', '4']
        self.months = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']
        self.wait = int(os.getenv('wait'))
        self.countries = os.getenv('countries').split(",")
        self.countries = [x.strip(' ') for x in self.countries]
        self.year = os.getenv('year').split(",")
        self.year = [x.strip(' ') for x in self.year]
        self.validate_wait()

        # Database related variables
        self.conn_string = "postgresql://"+os.getenv('user')+":"+os.getenv('password')+\
                           "@"+os.getenv('host') + ":"+os.getenv('port')+"/"+os.getenv('name')
        self.comtrade_data_table = os.getenv('comtrade_data_table')
        self.supervisor_table = os.getenv('supervisor_table')
        # self.data_range_table = os.getenv('data_range_table')
        self.db = create_engine(self.conn_string, pool_pre_ping=True)
                                #connect_args={"options": "-c statement_timeout=60000"})#

    def validate_wait(self):
        if int(self.wait < 0):
            print("Wait should not be a negative value. Please give a valid value.")
            sys.exit()

    def close_conn(self):
        try:
            self.db.dispose()
            print("DB connection closed successfully")
        except Exception as err:
            print(f'An error occurred: {err}')

    def supervisor(self):
        un1 = un.UN()
        db1 = db.DB()
        print("In supervisor table")
        for year in self.year:
            for country in self.countries:
                for month in self.months:
                    try:
                        print("Checking for record: rtCode:", country, ", ps:", year + month)
                        is_exist = db1.is_exist_in_supervisor(country, year, month)
                        if is_exist == True:
                            total_records, response = db1.get_response_and_totalrecord_from_supervisor(country, year,
                                                                                                       month)
                            if response == '<Response [200]>' and total_records >= 0:
                                # print(type(response))
                                # print("Record exists WITHOUT ERROR")
                                pass
                            else:
                                time.sleep(self.wait)
                                df, response = un1.get_trade_record_count(country, year, month)
                                if response == '<Response [200]>':
                                    db1.delete_record_from_supervisor(country, year, month)
                                    db1.write_data(df, self.supervisor_table, 'append', country, year, month)
                        else:
                            if is_exist == False:
                                time.sleep(self.wait)
                                # print("Record does not exist")
                                df, _ = un1.get_trade_record_count(country, year, month)
                                # print(df, "\n")
                                db1.write_data(df, self.supervisor_table, 'append', country, year, month)
                                # print("\n")
                    except Exception as err:
                        print(f'Error occurred 1: {err}')

    def comtrade_data(self):
        un2 = un.UN()
        db2 = db.DB()
        print("\nIn data table")
        for year in self.year:
            for month in self.months:
                for country in self.countries:
                    try:
                        print("Checking for record: rtCode:", country, "ps:", year + month)

                        all_response_is_empty = 0
                        already_exists = 0
                        call_trade_dataAPI_error = 0
                        alteast_one_exists = 0
                        total_records, response = db2.get_response_and_totalrecord_from_supervisor(country, year, month)
                        if total_records <= 0:
                            already_exists = -1
                        if response == '<Response [200]>' and total_records > 0:
                            for country2 in self.countries:
                                if country != country2:
                                    print("ptCode: " + country2)
                                    for flow in self.flow:
                                        is_exist = db2.is_exist_in_trade_data(country, year, month, country2, flow)
                                        if is_exist == False:
                                            time.sleep(self.wait)
                                            df, response = un2.get_trade_data(country, country2, flow, year, month)

                                            if str(response) == '<Response [200]>':
                                                if not df.empty:
                                                    all_response_is_empty = 1
                                                    db2.write_data(df, self.comtrade_data_table, 'append', country, year,
                                                                   month)
                                                    already_exists = 1
                                            else:
                                                # if str(response) != '<Response [200]>' and country=='36' and country2 =='360' \
                                                #         and flow == '1' and year == '2016' and month == '01':
                                                if str(response) != '<Response [200]>':
                                                    call_trade_dataAPI_error = -1
                                                    print("Failed to call API for rtCode:", country, ", ps:",
                                                          year + month, "ptCode:", country2)
                                        else:
                                            alteast_one_exists = 1

                            if call_trade_dataAPI_error == -1:
                                print("Failed to call data API for some records. Check logs")
                                comment_error = "Failed to call data API for some records. Check logs"
                                db2.update_comment_in_supervisor_table(country, year, month, comment_error)
                            else:
                                if already_exists == 1:
                                    print("Some records did not exist earlier, now added")
                                    comment_not_exist = "Records added"
                                    db2.update_comment_in_supervisor_table(country, year, month, comment_not_exist)
                                else:
                                    if all_response_is_empty == 0 and alteast_one_exists == 0:
                                        print("All responses are empty")
                                        comment_all_response_empty = "All responses are empty"
                                        db2.update_comment_in_supervisor_table(country, year, month, comment_all_response_empty)
                                    else:
                                        if already_exists == 0:
                                            # print("Record exists")
                                            comment_exists = "Record exists"
                                            db2.update_comment_in_supervisor_table(country, year, month, comment_exists)
                        # print("\n")
                    except Exception as err:
                        print(f'Error occurred 2: {err}')


# def comtrade(req):
#     obj1 = Comtrade()
#     obj1.close_conn()
#     return HttpResponse('')

obj2 = Comtrade()
obj2.supervisor()
obj2.comtrade_data()
obj2.close_conn()
