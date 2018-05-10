package com.kaviddiss.storm;

import com.mongodb.client.model.Filters;
import org.apache.storm.mongodb.common.QueryFilterCreator;
import org.apache.storm.tuple.ITuple;
import org.bson.conversions.Bson;

public class TweetsMongoQueryFilterCreator implements QueryFilterCreator {
    private String field;

    @Override
    public Bson createFilter(ITuple tuple) {
        return Filters.eq(field, tuple.getValueByField(field));
    }

    public TweetsMongoQueryFilterCreator withField(String field) {
        this.field = field;
        return this;
    }
}
