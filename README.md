# ApacheBeam-BigDataChallenge

1. Join both datasets in Beam

2. Do the following transformations:
2.1. Filter any duplicate rows
2.2. String concatenation of arrival and destination airport
2.3. Geohashing the latitude and longitude
2.4. String padding the path_order with trailing zeros to a total string length of 10

3. Calculate the following aggregates for every airline:
3.1. Total flights
3.2. Total flights per day

NOTE:
Coded task in Java.
Reduced the dataset sizes to run on local machine (using a DirectRunner in Beam)

RESOURCES:
1. https://beam.apache.org/get-started/beam-overview/
2. https://beam.apache.org/get-started/quickstart-java/
3. https://beam.apache.org/get-started/wordcount-example/
4. https://docs.google.com/presentation/d/1SHie3nwe-pqmjGum_QDznPr-B_zXCjJ2VBDGdafZme8/edit
5. https://beam.apache.org/documentation/
6. https://en.wikipedia.org/wiki/Geohash
