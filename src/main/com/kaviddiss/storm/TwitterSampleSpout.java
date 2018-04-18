/**
 * Taken from the storm-starter project on GitHub
 * https://github.com/nathanmarz/storm-starter/
 */
package com.kaviddiss.storm;

import com.kaviddiss.storm.tool.SentimentAnalyzer;
import org.apache.storm.Config;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Reads Twitter's sample feed using the twitter4j library.
 *
 * @author davidk
 */
@SuppressWarnings({"rawtypes", "serial"})
public class TwitterSampleSpout extends BaseRichSpout {

    private SpoutOutputCollector collector;
    private LinkedBlockingQueue<Status> queue;
    private TwitterStream twitterStream;

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        queue = new LinkedBlockingQueue<Status>(1000);
        SentimentAnalyzer.init();
        this.collector = collector;

        StatusListener listener = new StatusListener() {
            @Override
            public void onStatus(Status status) {
                queue.offer(status);
            }

            @Override
            public void onDeletionNotice(StatusDeletionNotice sdn) {
            }

            @Override
            public void onTrackLimitationNotice(int i) {
            }

            @Override
            public void onScrubGeo(long l, long l1) {
            }

            @Override
            public void onStallWarning(StallWarning stallWarning) {
            }

            @Override
            public void onException(Exception e) {
            }
        };

        // todo : read these keys from properties file
        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setDebugEnabled(true)
                .setOAuthConsumerKey("Z4fXwtIlLZg68GrHUgRkrgk8p")
                .setOAuthConsumerSecret("8g5cyEudgR197shO2xRlfCu1lfjTQixZVcZVhArqy2bMNWPWBW")
                .setOAuthAccessToken("132447463-kW0wdBUW0BlvjOIdEDDE5TA4kUvPrbJdEBWkdCh6")
                .setOAuthAccessTokenSecret("iGHEEYlkkxlPQoSWWLYB42yqkjoZ0oui7bCdkQy8i398Q");

        TwitterStreamFactory factory = new TwitterStreamFactory(cb.build());
        twitterStream = factory.getInstance();
        twitterStream.addListener(listener);

        //set up the filter
        FilterQuery fq = new FilterQuery();
        double[][] loc={{-107.31,25.68},{-93.25,36.7}};
        fq.locations(loc);
        twitterStream.filter(fq);

    }

    @Override
    public void nextTuple() {
        Status ret = queue.poll();
        if (ret == null) {
            Utils.sleep(50);
        } else {
            collector.emit(new Values(ret));
        }
    }

    @Override
    public void close() {
        twitterStream.shutdown();
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Config ret = new Config();
        ret.setMaxTaskParallelism(1);
        return ret;
    }

    @Override
    public void ack(Object id) {
    }

    @Override
    public void fail(Object id) {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("tweet"));
    }

}
