package com.kaviddiss.storm;

import com.kaviddiss.storm.tool.SentimentAnalyzer;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.GeoLocation;
import twitter4j.Place;
import twitter4j.Status;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * Receives tweets and emits its words over a certain length.
 */
public class WordFilter extends BaseRichBolt {

    private static final long serialVersionUID = 5151173513759399636L;

    private static final Logger logger = LoggerFactory.getLogger(WordFilter.class);

//    private final int minWordLength;

    private OutputCollector collector;

    public WordFilter() {

    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        this.collector = collector;
    }


    @Override
    public void execute(Tuple input) {
        String[] keyword = new String[] {"a","cool","LOL","cholesterol", " EKG ", "Aneurysm" ,"Angina" , "Angiogenesis" ,"Coronary Arteries",
                "Coronary" , " LDL " , " HDL " , "bypass surgery" , "steats" ,"high sugar level",
                "chest pain", "chest pressure", "difficulty breathing", "heart attack", "blood pressure", "cardiac arrest",
                "Shooting left arm pain", "arm pain", "shooting pain", "left arm tingling", "shortness of breath"};

        Status tweet = (Status) input.getValueByField("tweet");
        String lang = tweet.getUser().getLang();
        String text = tweet.getText();

        GeoLocation[][] boundingBoxCoordinates = tweet.getPlace().getBoundingBoxCoordinates();
        double lat_0 =  boundingBoxCoordinates[0][0].getLatitude();
        double lon_0 =  boundingBoxCoordinates[0][0].getLongitude();
        double lat_1 =  boundingBoxCoordinates[0][1].getLatitude();
        double lon_1 =  boundingBoxCoordinates[0][1].getLongitude();
        double lat_2 =  boundingBoxCoordinates[0][2].getLatitude();
        double lon_2 =  boundingBoxCoordinates[0][2].getLongitude();
        double lat_3 =  boundingBoxCoordinates[0][3].getLatitude();
        double lon_3 =  boundingBoxCoordinates[0][3].getLongitude();
        String placeName = tweet.getPlace().getName();

        double lat_point = ( lat_0 + lat_1 + lat_2 + lat_3 ) / 4;
        double lon_point = ( lon_0 + lon_1 + lon_2 + lon_3 ) / 4;

        GeoLocation pointCoordinates = new GeoLocation(lat_point,lon_point);

        String json = "{ " +
                "'type': 'Feature', " +
                "'geometry': { " +
                "'type': 'Point', " +
                "'coordinates': [" + lat_point + "," + lon_point + "] " +
                "}, " +
                "'properties': { " +
                "'name': '" + placeName + "' " +
                "} " +
                "}";

        DBObject dbObject = (DBObject)JSON.parse(json);

        //remove hash tag
        String textWithoutHashTag = text.replace("#", "");
        //Create Regex pattern to find urls
        Pattern urlPattern = Pattern.compile("http\\S+");
        //Create a matcher with our 'urlPattern'
        Matcher matcher = urlPattern.matcher(textWithoutHashTag);
        //Check if matcher finds url
        String tweetWithoutHashTagAndUrl;

        if(matcher.find()) {
            //Matcher found urls
            //Removing them now..
            tweetWithoutHashTagAndUrl = matcher.replaceAll("");
            //Use new tweet here
        } else {
            //Matcher did not find any urls, which means the 'tweetWithoutHashtag' already is ready for further usage
            tweetWithoutHashTagAndUrl = textWithoutHashTag;
        }

        for (String s : keyword){
            if (tweetWithoutHashTagAndUrl.toLowerCase().contains(s.toLowerCase()))
            {
                String characterFilter = "[^\\p{L}\\p{M}\\p{N}\\p{P}\\p{Z}\\p{Cf}\\p{Cs}\\s]";
                String emotionless = tweetWithoutHashTagAndUrl.replaceAll(characterFilter,"");
                int sentiment = SentimentAnalyzer.findSentiment(emotionless)-2;
//                logger.info(String.valueOf(new StringBuilder("tweet - ").append(lang).append('|').append(emotionless).append('|').append(sentiment)));
                collector.emit(new Values(emotionless,dbObject,sentiment));

//                BufferedWriter output;
//                try {
//                    output = new BufferedWriter(new FileWriter("C:/F/STADY/expasome/storm-twitter-word-count/src/main/com/kaviddiss/storm/output/geoInfo.cvs", true));
//                    output.newLine();
//                    output.append("text: ").append(emotionless).append(", ").append("boundingBoxCoordinates: ")
//                            .append(Arrays.deepToString(boundingBoxCoordinates)).append(", ").append("score: ").append(String.valueOf(sentiment));
//                    output.close();
//                } catch (IOException e) {
//                    e.printStackTrace();
//                }
                break;
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("text", "geolocation", "score"));
    }
}
