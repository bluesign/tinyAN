import requests
import json


def makeRequest(method, params, url="http://cdnc.dev/"):
    p = {
        "method": "",
        "params": [],
        "id": 1,
        "jsonrpc": "2.0",
    }
    headers = {"Content-Type": "application/json"}

    p["method"] = method
    p["params"] = params
    response = requests.request("POST", url, headers=headers, data=json.dumps(p))
    return response.json()


for i in range(4582365, 1, -1):
    print(i, hex(i))

    r = makeRequest("debug_traceBlockByNumber", [hex(i)])

