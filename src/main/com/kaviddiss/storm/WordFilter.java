package com.kaviddiss.storm;

import com.kaviddiss.storm.tool.SentimentAnalyzer;
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
        String[] keyword = new String[] {"trump","chinese","china","chink","cholesterol", " EKG ", "Aneurysm" ,"Angina" , "Angiogenesis" ,"Coronary Arteries",
                "Coronary" , " LDL " , " HDL " , "bypass surgery" , "steats" ,"high sugar level",
                "chest pain", "chest pressure", "difficulty breathing", "heart attack", "blood pressure", "cardiac arrest",
                "Shooting left arm pain", "arm pain", "shooting pain", "left arm tingling", "shortness of breath"};
        Status tweet = (Status) input.getValueByField("tweet");
        String lang = tweet.getUser().getLang();
        String text = tweet.getText();
//        GeoLocation[][] boundingBoxCoordinates = tweet.getPlace().getBoundingBoxCoordinates();

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
                int sentiment = SentimentAnalyzer.findSentiment(tweetWithoutHashTagAndUrl)-2;
                logger.info(String.valueOf(new StringBuilder("tweet - ").append(lang).append('|').append(tweetWithoutHashTagAndUrl).append('|').append(sentiment)));
                break;
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("lang", "word"));
    }
}
