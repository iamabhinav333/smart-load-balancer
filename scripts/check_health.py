import requests

def check(url):
    try:
        r = requests.get(url, timeout=3)
        print(f"{url} {r.status_code} {r.json()}")
    except Exception as e:
        print(f"{url} ERROR {e}")

if __name__ == '__main__':
    check('http://127.0.0.1:5001/health')
    check('http://127.0.0.1:5002/health')
