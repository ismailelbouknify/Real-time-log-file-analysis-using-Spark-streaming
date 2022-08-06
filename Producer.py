from kafka import KafkaProducer
from time import sleep
import json
import pandas as pd
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],api_version=(0,10,1))

# Ouvrir le fichier en lecture seule
file = open('C:\\Users\\hp\BDSaS\\Big_Data\\dataset\\access.log', "r")
# utiliser readlines pour lire toutes les lignes du fichier
# La variable "lignes" est une liste contenant toutes les lignes du fichier
lines = file.readlines()
# fermez le fichier après avoir lu les lignes
file.close()

for line in lines:
    message = line
    producer.send('test',json.dumps(message).encode('utf-8'))
    sleep(1/100)