class APIProxy:
    def __init__(self, testNo):
        self.testNo = testNo

    def is_response_error(self, response):
        return not str(response) == '<Response [200]>'

    def get_response(self, url):
        json = ""

        # match testNo:
        #     case 1:
        #         # json = "[{"type":null,"freq":null,"px":null,"r":null,"rDesc":null,"ps":null,"TotalRecords":0,"isOriginal":null,"publicationDate":null,"isPartnerDetail":0}]"
        #
        # return json, response