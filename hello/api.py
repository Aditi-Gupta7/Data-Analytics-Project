import requests

class API:
    def is_response_error(self, response):
        return not str(response) == '<Response [200]>'

    def get_response(self, url):
        response = ''
        data = ''
        try:
            response = requests.get(url, timeout=20)
            response.raise_for_status()
            data = response.json()
        except requests.exceptions.Timeout:
            response = 'Error'
            print("--Timeout Exception for --", url)
        except Exception as err:
            response = 'Error'
            print(f'Other error occurred 3: {err}', url)
        return data, response