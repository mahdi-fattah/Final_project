import schedule
import time
import os


def run_code():
    os.system("python3 query.py")
schedule.every(30).minutes.do(run_code)
while True:
    schedule.run_pending()
    time.sleep(1)
