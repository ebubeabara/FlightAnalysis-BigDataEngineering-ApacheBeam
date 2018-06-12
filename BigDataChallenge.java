import java.util.Arrays;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.values.*;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.*;
import com.github.davidmoten.geo.GeoHash;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Distinct;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
/**
 * Created by EBUBECHUKWU on 25/02/2018.
 */
public class BigDataChallenge {
    public static void main(String[] args){
        String flightsFile = "C:\\Users\\EBUBECHUKWU\\Desktop\\datatonic\\flights_small.csv";
        String weatherFile = "C:\\Users\\EBUBECHUKWU\\Desktop\\datatonic\\weather.csv";
        String flightsFIleWithoutHeader = "C:\\Users\\EBUBECHUKWU\\Desktop\\datatonic\\flights_small_WithoutHeader.csv";
        GeoHash geoHash = null;

        ////JOIN BOTH DATASETS
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);
        PCollection<String> flights_small = pipeline.apply(TextIO.read().from(flightsFile));
        PCollection<String> weather = pipeline.apply(TextIO.read().from(weatherFile));
        PCollectionList<String> collections = PCollectionList.of(flights_small).and(weather);
        PCollection<String> merged = collections.apply(Flatten.<String>pCollections());


        ////TRANSFORMATIONS
        PipelineOptions optionsTransformation = PipelineOptionsFactory.create();
        Pipeline pipelineTransformation = Pipeline.create(optionsTransformation);
        pipelineTransformation.apply(TextIO.read().from(flightsFIleWithoutHeader))
                .apply(Distinct.create())//REMOVES DUPLICATE ROWS
                .apply("ExtractWords", FlatMapElements
                        .into(TypeDescriptors.strings())
                        .via((word) -> Arrays.asList(
                                word.split(",")[0]
                                        + "," + word.split(",")[1] + "," + word.split(",")[2]
                                        + ",[" + word.split(",")[3] + "][" + word.split(",")[5] //STRING CONCATENATION OF ARRIVAL AND DESTINATION AIRPORT
                                        + "]," + word.split(",")[4] + "," + word.split(",")[6]
                                        + "," + word.split(",")[7] + "," + word.split(",")[8]
                                        + "," + word.split(",")[9] + "," + word.split(",")[10]
                                        + "," + word.split(",")[11] + "," + word.split(",")[12]
                                        + "," + word.split(",")[13] + "," + word.split(",")[14]
                                        + "," + geoHash.encodeHash(Double.parseDouble(word.split(",")[16]), Double.parseDouble(word.split(",")[15])) //LAT LONG GEO-HASH
                                        + "," + word.split(",")[17] + "," + word.split(",")[18] + "000000000" //TRAILING ZEROS
                        )))
                .apply(TextIO.write().to("Flights_Small_Transformed"));
        pipelineTransformation.run(optionsTransformation).waitUntilFinish();


        ////TOTAL FLIGHTS AGGREGATE FOR EVERY AIRLINE
        PipelineOptions optionsTotalFlightsPerAirline = PipelineOptionsFactory.create();
        Pipeline pipelineTotalFlightsPerAirline = Pipeline.create(optionsTotalFlightsPerAirline);
        pipelineTotalFlightsPerAirline.apply(TextIO.read().from(flightsFIleWithoutHeader))
                .apply(Distinct.create())//REMOVE DUPLICATES
                .apply("ExtractWords", FlatMapElements
                        .into(TypeDescriptors.strings())
                        .via ((word) -> Arrays.asList(word.split(",")[1])))
                .apply(Count.<String>perElement())
                .apply("FormatResults", MapElements
                        .into(TypeDescriptors.strings())
                        .via((KV<String, Long> wordCount) -> wordCount.getKey() + ": " + wordCount.getValue()))
                .apply(TextIO.write().to("Total_Flights_Per_Airline"));
        pipelineTotalFlightsPerAirline.run(optionsTotalFlightsPerAirline).waitUntilFinish();


        ////TOTAL FLIGHTS AGGREGATE FOR EVERY AIRLINE PER DAY
        PipelineOptions optionsTotalFlightsPerAirlinePerDay = PipelineOptionsFactory.create();
        Pipeline pipelineTotalFlightsPerAirlinePerDay = Pipeline.create(optionsTotalFlightsPerAirlinePerDay);
        pipelineTotalFlightsPerAirlinePerDay.apply(TextIO.read().from(flightsFIleWithoutHeader))
                .apply(Distinct.create())//REMOVE DUPLICATES
                .apply("ExtractWords", FlatMapElements
                        .into(TypeDescriptors.strings())
                        .via ((word) -> Arrays.asList(word.split(",")[0] + "," + word.split(",")[1])))
                .apply(Count.<String>perElement())
                .apply("FormatResults", MapElements
                        .into(TypeDescriptors.strings())
                        .via((KV<String, Long> wordCount) -> wordCount.getKey() + ": " + wordCount.getValue()))
                .apply(TextIO.write().to("Total_Flights_Per_Airline_Per_Day"));
        pipelineTotalFlightsPerAirlinePerDay.run(optionsTotalFlightsPerAirlinePerDay).waitUntilFinish();
    }


    ////BETTER JAVA METHOD TO IMPLEMENT TRAILING ZEROS
    public static String pathOrderTrailingZeros(String pathOrder){
        StringBuffer stringBuffer = new StringBuffer(10).append(pathOrder);
        try{
            int maxLength = 10;
            while(stringBuffer.length() <= maxLength){
                stringBuffer.append("0");
                maxLength++;
            }
            return stringBuffer.toString();
        } catch (Exception e){
            e.printStackTrace();
        } finally {
            return stringBuffer.toString();
        }
    }
}