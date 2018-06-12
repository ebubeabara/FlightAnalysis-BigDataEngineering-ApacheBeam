import csv

counter = 0
with open("weather.csv") as csvFile:
    for row in csv.reader(csvFile, delimiter=','):
        print(row)
        counter += 1
        if counter == 10:
            break