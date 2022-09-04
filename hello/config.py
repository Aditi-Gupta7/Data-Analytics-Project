from environment import Environment
from db import DB


class Config:
    def __init__(self):
        self.flow = ['1', '2', '3', '4']
        self.months = ['01', '02']#, '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']

        self.env = Environment()
        self.db = DB(self.env.conn_string, self.env.supervisor_table, self.env.comtrade_data_table)

