package com.mlnotes.storm.hotwords;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Zhu Hanfeng <me@mlnotes.com>
 * @date 2014-6-3 16:48:08
 */
public class DummyWordCounter extends BaseBasicBolt {
    public static Logger LOG = LoggerFactory.getLogger(DummyWordCounter.class);
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        String word = input.getString(0);
        LOG.info("Get Word: " + word);
    }
}
