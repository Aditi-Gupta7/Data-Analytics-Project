import pandas as pd
from hello.api import API


class UN(API):
    # Convert JSON to dataframe
    def to_dataframe(self, json, response, rec_path=None):
        df = pd.DataFrame()
        if str(response) == '<Response [200]>':
            df = pd.DataFrame(pd.json_normalize(json, record_path=rec_path)) # Empty df gives error
        return df

    def add_drop_columns_to_trade_data_count(self, response, df, country='None', year='None', month='None'):
        try:
            response = str(response)
            if response != '<Response [200]>':
                df = df.assign(ps=[year + month], r=[country], TotalRecords=[-1])
            else:
                if response == '<Response [200]>':
                    if df.empty:
                        df = df.assign(ps=[year + month], r=[country], TotalRecords=[0])
                    else:
                        df = df.drop(
                            ['type', 'freq', 'px', 'rDesc', 'isOriginal', 'publicationDate',
                             'isPartnerDetail'], axis=1,
                            errors='ignore')
                        if not isinstance(df['ps'][0], str): #or not isinstance(df['r'][0], int):
                            print("None value returned for all columns")
                            df = df.assign(ps=[year + month], r=[country], TotalRecords=[-1])
            df['Response'] = response
        except Exception as err:
            print(f'Error occurred 4: {err}')
        return df, response

    def get_trade_record_count(self, country, year, month):
        try:
            # Generate URl and collect response in a dataframe
            # time.sleep(5)
            data_avail_url = "http://comtrade.un.org/api//refs/da/view?parameters&type=C&freq=M&r=" \
                             + country + "&ps=" + year + month
            # print(data_avail_url)
            json, response = self.get_response(data_avail_url)
            df = self.to_dataframe(json, response)
            # print(df,"\n")
            return self.add_drop_columns_to_trade_data_count(response, df, country, year, month)
        except Exception as err:
            print(f'Error occurred 5: {err}')

    # Part 2
    def drop_columns_trade_data(self, df, response):
        df = df.drop(
            ['pfCode', 'periodDesc', 'IsLeaf', 'rgDesc', 'rtTitle', 'rt3ISO', 'ptTitle',
             'pt3ISO'
                , 'ptCode2', 'ptTitle2', 'pt3ISO2', 'cstCode', 'cstDesc', 'motCode', 'motDesc',
             'cmdDescE', 'qtCode', 'qtDesc', 'qtAltCode', 'qtAltDesc',
             'TradeQuantity', 'AltQuantity', 'GrossWeight',
             'CIFValue', 'FOBValue', 'estCode'], axis=1, errors='ignore')
        return df, response

    def get_trade_data(self, country, country2, flow, year, month):
        try:
            comtrade_url = "https://comtrade.un.org/api/get?type=C&freq=M&px=HS&ps=" + year + month + "&r=" + \
                            country + "&p=" + country2 + "&rg=" + flow + "&cc=ALL"
            # print(comtrade_url)
            # time.sleep(5)
            json, response = self.get_response(comtrade_url)
            df = self.to_dataframe(json, response, 'dataset')
            df, response = self.drop_columns_trade_data(df, response)
            return self.drop_columns_trade_data(df, response)
        except Exception as err:
            print(f'Error occurred 6: {err}')

