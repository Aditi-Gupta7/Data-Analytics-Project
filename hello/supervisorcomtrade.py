from config import Config


class Comment():
    def __init__(self):
        self.FAIL = "Failed to call data API for some records. Check logs"
        self.RecordsAdded = "Records added"
        self.AllEmpty = "All responses are empty"
        self.AllExists = "Record already exists"


class SupervisorComtrade():

    def __init__(self, country, year, month):
        self.country = country
        self.year = year
        self.month = month
        self.is_call_trade_data_api_error = False
        self.record_added = False
        self.is_atleast_one_exist = False

        config = Config()
        self.db = config.db

    def get_response_and_totalrecord(self, country, year, month):
        return self.db.get_response_and_totalrecord_from_supervisor(country, year, month)

    def __update_comment(self, comment, country, year, month):
        self.db.update_comment_in_supervisor_table(country, year, month, comment)

    def log_comment(self, country, year, month):
        comment = Comment()
        if self.is_call_trade_data_api_error:
            self.__update_comment(comment.FAIL, country, year, month)
        else:
            if self.record_added:
                self.__update_comment(comment.RecordsAdded, country, year, month)
            else:
                if self.record_added is False and self.is_atleast_one_exist is False:
                    self.__update_comment(comment.AllEmpty, country, year, month)
                else:
                    if not self.record_added:
                        self.__update_comment(comment.AllExists, country, year, month)

    def report_is_complete(self):
        self.log_comment(self.country, self.year, self.month)
