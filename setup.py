import os
import datetime
import random

# Commented out if running locally

# from dotenv import load_dotenv

def application_data():
    print('application_data')
    data = {
        "version": "0.0.1",
        "environment": os.getenv("ENVIRONMENT"),
        "date": datetime.datetime.now()
    }
    return data

def application_running():
    randomlist = []
    for _ in range(0, 5):
        n = random.randint(1, 30)
        randomlist.append(n)
    return randomlist, 'ok'

def init(health, env_dump):
    # load_dotenv()
    health.add_check(application_running)
    env_dump.add_section("application", application_data)
