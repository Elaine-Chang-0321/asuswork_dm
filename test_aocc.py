import requests
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

url = "https://stage-iotapi.asus.com/aoccgpt2/v1/openapi/auth"
token = "FD2D00F6-A33A-4226-87B0-24ED2A94C90E"
headers = {"Authorization": token}

print("Test 1: params=None")
try:
    resp1 = requests.get(url, headers=headers, verify=False)
    print(resp1.status_code, resp1.text)
except Exception as e:
    print(e)

print("\nTest 2: params={'key': token}")
try:
    resp2 = requests.get(url, headers=headers, params={"key": token}, verify=False)
    print(resp2.status_code, resp2.text)
except Exception as e:
    print(e)

print("\nTest 3: POST body={'key': token}")
try:
    resp3 = requests.post(url, headers=headers, json={"key": token}, verify=False)
    print(resp3.status_code, resp3.text)
except Exception as e:
    print(e)
