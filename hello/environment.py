import os

class Environment:

    def __init__(self):
        self.wait = int(os.getenv('wait'))
        self.countries = os.getenv('countries').split(",")
        self.countries = [x.strip(' ') for x in self.countries]
        self.years = os.getenv('year').split(",")
        self.years = [x.strip(' ') for x in self.years]

        # Database
        self.conn_string = "postgresql://" + os.getenv('user') + ":" + os.getenv('password') + \
                           "@" + os.getenv('host') + ":" + os.getenv('port') + "/" + os.getenv('name')
        self.comtrade_data_table = os.getenv('comtrade_data_table')
        self.supervisor_table = os.getenv('supervisor_table')