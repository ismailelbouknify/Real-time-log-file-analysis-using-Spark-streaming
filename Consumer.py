cfrom kafka import KafkaConsumer
from time import sleep
import json
import re
import pandas as pd

consumer = KafkaConsumer('test',bootstrap_servers=['localhost:9092'],api_version=(0,10))

# There is a minor bug in this regex, it misses the last field. I'll fix this soon. 

common_regex = '^(?P<client>\S+) \S+ (?P<userid>\S+) \[(?P<datetime>[^\]]+)\] "(?P<method>[A-Z]+) (?P<request>[^ "]+)? HTTP/[0-9.]+" (?P<status>[0-9]{3}) (?P<size>[0-9]+|-)'
combined_regex = '^(?P<client>\S+) \S+ (?P<userid>\S+) \[(?P<datetime>[^\]]+)\] "(?P<method>[A-Z]+) (?P<request>[^ "]+)? HTTP/[0-9.]+" (?P<status>[0-9]{3}) (?P<size>[0-9]+|-) "(?P<referrer>[^"]*)" "(?P<useragent>[^"]*)'
columns = ['client', 'userid', 'datetime', 'method', 'request', 'status', 'size', 'referer', 'user_agent']


def get_dic(line):
    
    dic={'client':str(line[0]),'user_id':str(line[1]),'datetime':pd.to_datetime(str(line[2]),format='%d/%b/%Y:%H:%M:%S %z'),'method':str(line[3]),'status':str(line[4]),'size':str(line[5]),'referer':str(line[6]),'user_agent':str(line[7])}
    return dic
    
import pymongo
def connect_db():
    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    mydb = myclient["logs_db"]
    mycoll = mydb["logs"]
    return mycoll
    
def insert_line(line,mycol):
    try:
        log_line = re.findall(combined_regex, line)[0]
        dic = get_dic(log_line)
    except Exception as e:
        print()
    mycol.insert_one(dic) 
        
mycol = connect_db()

for message in consumer:
    line = message.value
    insert_line(line,mycol)        