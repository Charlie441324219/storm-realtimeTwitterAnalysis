package com.kaviddiss.storm;

import org.apache.storm.mongodb.common.mapper.MongoMapper;
import org.apache.storm.mongodb.common.mapper.SimpleMongoUpdateMapper;
import org.apache.storm.tuple.ITuple;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

public class TweetsMongoMapper implements MongoMapper {
    private String[] fields;
    private static final Logger logger = LoggerFactory.getLogger(TweetsMongoMapper.class);

    @Override
    public Document toDocument(ITuple tuple) {
        Document document = new Document();
        for(String field : fields){
            document.append(field, tuple.getValueByField(field));
        }
//        logger.info(String.valueOf(new StringBuilder("222tweet - ").append(document)));
        return document;
    }

    public TweetsMongoMapper withFields(String... fields) {
        this.fields = fields;
        return this;
    }
}
