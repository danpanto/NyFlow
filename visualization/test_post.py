import urllib.request
import json
req = urllib.request.Request('http://localhost:8000/api/restaurant-ratings', method='POST')
req.add_header('Content-Type', 'application/json')
body = json.dumps({"variables":[],"vendors":[],"date":{"min":"2021-01-01T00:00:00","max":"2025-12-31T23:00:00"}})
try:
    res = urllib.request.urlopen(req, data=body.encode('utf-8'))
    print(res.read().decode('utf-8')[:500])
except Exception as e:
    print(e)
