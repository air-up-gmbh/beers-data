from datetime import datetime

# https://medium.com/@django.course/7-ways-to-execute-scheduled-jobs-with-python-47d481d22b91

# crontab -e
# * 3 * * * /usr/bin/python /home/jay/workspace/air-up/beers-data/cronjob_test.py > /home/jay/workspace/air-up/logs/`date +\%Y\%m\%d\%H\%M\%S`-cron.log 2>&1

if __name__ == '__main__':
    print("Dummy Job running at {}".format(str(datetime.now())))
