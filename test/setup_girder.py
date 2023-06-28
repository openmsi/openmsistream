#!/usr/bin/env python3
import sys
import time
import requests

params = {
    "login": "admin",
    "email": "root@dev.null",
    "firstName": "John",
    "lastName": "Doe",
    "password": "arglebargle123",
    "admin": True,
}
headers = {"Content-Type": "application/json", "Accept": "application/json"}
api_url = "http://localhost:8080/api/v1"

# Give girder time to start
while True:
    print("Waiting for Girder to start")
    r = requests.get(api_url, timeout=10)
    if r.status_code == 200:
        break
    time.sleep(2)

print("Creating admin user")
r = requests.post(f"{api_url}/user", params=params, headers=headers, timeout=10)
if r.status_code == 400:
    print("Admin user already exists. Database was not purged.")
    sys.exit()

# Store token for future requests
headers["Girder-Token"] = r.json()["authToken"]["token"]

print("Creating default assetstore")
r = requests.post(
    f"{api_url}/assetstore",
    headers=headers,
    params={
        "type": 0,
        "name": "Base",
        "root": "/srv/data/base",
    },
    timeout=10,
)
