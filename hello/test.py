from config import Config
from api import API
from hello.un import UN
from hello.comtrade import Comtrade
from hello.config import Config
from hello.supervisorcomtrade import SupervisorComtrade

# Test API.py
# api = API()
# _, response = api.get_response("https://comtrade.un.org/api//refs/da/view?parameters&type=C&freq=M&r=2&ps=201601")
# print(response)
# print(api.is_response_error(response))


config = Config()
un = UN()
comtrade = Comtrade()
sc = SupervisorComtrade('36', '2016', '01')
df, response = un.get_trade_data('36', '360', '1', '2016', '01')
# print(df)
print(response)
comtrade.process_data_api_response(response, df, config.db, '36', '360', '2016', '01', sc)


# config = Config()
# main = Main()
# # main.supervisor()
#
# main.comtrade_data()
# main.close_conn()

