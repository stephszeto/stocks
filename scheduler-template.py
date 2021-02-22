import schedule
import time

def job():
	print("Pulling data")
	stream = open("overview-data.py")
	file = stream.read()
	exec(file)

def eod_job():
	job()

	stream = open("insider-data.py")
	file = stream.read()
	exec(file)

schedule.every().day.at("08:00").do(job)
schedule.every().day.at("11:00").do(job)
schedule.every().day.at("12:00").do(job)
schedule.every().day.at("13:30").do(eod_job)

while True:
	print("Checking ...")
	schedule.run_pending()
	time.sleep(30)