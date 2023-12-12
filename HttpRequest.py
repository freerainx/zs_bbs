import httpx
import json

'''
#字典转换成json字符串 
json.dumps(dict)

#格式化输出
json.dumps(dic, sort_keys=True, indent=4, separators=(',', ':'))

#json字符串转换成字典
json.loads(json_str)
'''


class HttpRequest:
    def __init__(self, url, **kwargs):
        self.url = url
        self.json_str = ""
        self.response = None
        self.auth = ""
        self.content = ""
        self.token = ""

    def setJson(self, json_str):
        self.json_str = json_str

    def showJson(self):
        print(json.dumps(self.json_str, sort_keys=True, indent=4, separators=(',', ':')))

    def request(self, mode="post"):
        self.auth = ""
        self.content = ""
        self.token = ""
        self.response = httpx.request(mode, self.url, json=self.json_str)

        self.auth = self.response.headers.get("Authorization")
        self.content = self.response.text

        if len(self.content) > 0:
            dict = json.loads(self.content)
            print(type(dict))
            '''
            if dict is not None:
                if dict.has_key("token"):
                    self.token = dict["token"]
            '''

    def getStatusCode(self):
        if self.response is not None:
            return self.response.status_code
        return 0

    def setToken(self, token):
        this.token = token

    def getToken(self):
        return self.token

    def getContent(self):
        return self.content

    def showResponse(self):
        print("status --> ", self.response.status_code)
        print("header --> ", self.response.headers)
        print("auth   --> ", self.auth)
        print("text   --> ", self.content)
        print("token  --> ", self.token)

        """

        auth_lst = str.split(self.token , ".")
        for s in auth_lst:
            print(s)
        """


if __name__ == "__main__":

    url: str = "http://localhost:8081/user/login"
    data = {"user_name": "user5", "password": "password5"}

    # url: str = "http://localhost:8081/api/authenticate"
    # data = {"username": "email5", "password": "password5"}

    # url = "http://localhost:8080/api/v1/auth/register"
    # data = {"firstname":"freerain2","lastname":"Tse","email":"freerain2@live.cn","password":"qwer1234"}

    # url = "http://localhost:8080/api/v1/auth/authenticate"
    # data = {"email":"freerain2@live.cn","password":"qwer1234"}

    request = HttpRequest(url)
    request.setJson(data)
    request.showJson()

    request.request()
    if request.getStatusCode() != 0:
        request.showResponse()

    print(request.response.cookies)
    print(type(request.response.cookies))
    # print(request.response.cookies.__base__)
    # print(request.response.cookies.__class__)

    for ck in request.response.cookies:
        print(ck)

'''

'''

'''
client = httpx.Client()
client.headers["Authentication"]="asdfsdfasdfasdfdsafasdfasdfs"
print(client.headers)
'''


