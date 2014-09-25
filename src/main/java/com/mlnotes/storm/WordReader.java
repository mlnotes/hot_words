package com.mlnotes.storm;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import java.util.Map;
import org.joda.time.DateTime;
import redis.clients.jedis.Jedis;

/**
 *
 * @author zhf
 * @date 2014-3-5 20:45:27
 */
public class WordReader extends BaseRichSpout{

    private SpoutOutputCollector collector;
    private Jedis jedis;
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("line"));
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        this.jedis = new Jedis(Constants.REDIS_SERVER);
    }

    @Override
    public void nextTuple() {
        String weibo = jedis.lpop(Constants.QUEUE_NAME);
        if(weibo != null && weibo.length() > 0){
            this.collector.emit(new Values(weibo));
        }
    }
}
