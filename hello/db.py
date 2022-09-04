import os
import time
from sqlalchemy.exc import IntegrityError
from psycopg2.errors import UniqueViolation
from sqlalchemy import create_engine
from sqlalchemy import text
from dotenv import load_dotenv

load_dotenv()


class DB:
    def __init__(self, conn_string, supervisor_table, trade_data_table):
        self.supervisor_table = supervisor_table
        self.db = create_engine(conn_string)
        self.trade_data = trade_data_table

    def is_response_ok(self, response):
        return response == '<Response [200]>'

    def execute_query(self, query):
        sql = text(query)
        return self.db.execute(sql)

    # Tell if P.K exists in supervisor
    def is_exist_in_supervisor(self, country, year, month):
        # QUERY EX: 'SELECT EXISTS(SELECT "Response" from data_avail_api_report where r=36 and ps = 201801)'
        query = "SELECT EXISTS(SELECT \"Response\" from \"" + self.supervisor_table + \
                "\" WHERE r=" + country + "and ps=" + year + month + ")"
        result = self.execute_query(query)
        for record in result:
            if record[0]:
                return True
            else:
                return False

    # Write data to supervisor table
    def write_data(self, df, table_name, ifexists, country, year, month):
        try:
            # time.sleep(5)
            # print(df)
            df.to_sql(table_name, con=self.db, if_exists=ifexists, index=False)
            return True
        except IntegrityError as e:
            if isinstance(e.orig, UniqueViolation):
                print("--Unique Value Violation--")
        except Exception as err:
            print(f'Error occurred while writing to database: {err}')
            return False

    # Get value stored Response column of Supervisor table
    def get_response_from_supervisor(self, country, year, month):
        query = "SELECT \"Response\" from \"" + self.supervisor_table + "\" WHERE r=" + country + \
                "and ps=" + year + month
        result = self.execute_query(query)
        record = [record[0] for record in result]
        return record[0]

    # Delete record from supervisor table
    def delete_record_from_supervisor(self, country, year, month):
        query = "DELETE from \"" + self.supervisor_table + "\" WHERE r=" + country + \
                "and ps=" + year + month
        self.execute_query(query)

    # PART 2
    def get_response_and_totalrecord_from_supervisor(self, country, year, month):
        query = "SELECT \"Response\", \"TotalRecords\" from \"" + self.supervisor_table + "\" WHERE r=" + country + \
                "and ps=" + year + month
        result = self.execute_query(query)
        response = ''
        total_records = ''
        for e in result:
            response = e[0]
            total_records = e[1]
        return total_records, response

    # Tell if rtcode, ptcode, period exist in trade_data
    def is_exist_in_trade_data(self, country1, country2, year, month, flow):
        query = "SELECT EXISTS(SELECT \"period\" from \"" + self.trade_data + \
                "\" WHERE \"rtCode\"=" + country1 + " and period = " + year + month + " and \"ptCode\"=" + country2 \
                + " and \"rgCode\"=" + flow + ")"
        result = self.execute_query(query)
        for record in result:
            if record[0]:
                return True
            else:
                return False

    def update_comment_in_supervisor_table(self, country, year, month, comment):
        query = "UPDATE \"" + self.supervisor_table + "\" SET \"Comments\" = \'" + comment + "\' WHERE r = " + country + \
                " and ps = " + year + month
        self.execute_query(query)
