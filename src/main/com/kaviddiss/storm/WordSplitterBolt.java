package com.kaviddiss.storm;

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

/**
 * Receives tweets and emits its words over a certain length.
 */
public class WordSplitterBolt extends BaseRichBolt {

    private static final long serialVersionUID = 5151173513759399636L;

    private static final Logger logger = LoggerFactory.getLogger(WordSplitterBolt.class);

    private final int minWordLength;

    private OutputCollector collector;

    public WordSplitterBolt(int minWordLength) {
        this.minWordLength = minWordLength;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        this.collector = collector;
    }


    @Override
    public void execute(Tuple input) {
        String[] keyword = new String[] {"trump","cholesterol", "EKG", "Aneurysm" ,"Angina" , "Angiogenesis" ,"Coronary Arteries",
                "Coronary" , "LDL" , "HDL" , "bypass surgery" , "steats" ,"high sugar level",
                "chest pain", "chest pressure", "difficulty breathing", "heart attack", "blood pressure", "cardiac arrest",
                "Shooting left arm pain", "arm pain", "shooting pain", "left arm tingling", "shortness of breath"};
        Status tweet = (Status) input.getValueByField("tweet");
        String lang = tweet.getUser().getLang();
//        String text = tweet.getText().replaceAll("\\p{Punct}", " ").replaceAll("\\r|\\n", "").toLowerCase();
        String text = tweet.getText();
        GeoLocation[][] boundingBoxCoordinates = tweet.getPlace().getBoundingBoxCoordinates();
        for (String s : keyword){
            if (text.toLowerCase().contains(s.toLowerCase()))
            {
                logger.info(String.valueOf(new StringBuilder("tweet - ").append(lang).append('|').append(text)));
                break;
            }
        }

//        String[] words = text.split(" ");
//        for (String word : words) {
//            if (word.length() >= minWordLength) {
//                collector.emit(new Values(lang, word));
//                if(lang.equals("en") && boundingBoxCoordinates != null)
//                    logger.info(String.valueOf(new StringBuilder("top - ").append(lang).append('|').append(word).append('|').append(Arrays.deepToString(boundingBoxCoordinates))));
//            }
//        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("lang", "word"));
    }
}
