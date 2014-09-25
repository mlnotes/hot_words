package com.mlnotes.storm;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author zhf
 * @date 2014-3-5 21:00:08
 */
public class WordCounter extends BaseBasicBolt{
    private DBCollection coll;
    
    @Override
    public void prepare(Map conf, TopologyContext context){
        try {
            MongoClient client = new MongoClient("202.120.40.181");
            DB db = client.getDB("zhf");
            coll = db.getCollection("storm_mlnotes");
        } catch (UnknownHostException ex) {
            Logger.getLogger(WordCounter.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        String word = input.getString(0);
        if(coll == null){
            return;
        }
        
        BasicDBObject query = new BasicDBObject("w", word);
        BasicDBObject toIncrement = new BasicDBObject("c", 1);
        BasicDBObject update = new BasicDBObject("$inc", toIncrement);
        
        coll.update(query, update, true, false);
        
        System.out.println("Word: " + word);
    }
}