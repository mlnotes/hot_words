package com.mlnotes.storm.hotwords;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Find the hot words in weibo
 *
 */
public class App 
{
    public static Logger LOG = LoggerFactory.getLogger(App.class);
    
    public static void main( String[] args )
    {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("word-reader", new WordReader())
                .setNumTasks(10);
        builder.setBolt("word-spliter", new WordSpliter())
                .shuffleGrouping("word-reader")
                .setNumTasks(100);
        builder.setBolt("word-counter", new DummyWordCounter())
                .fieldsGrouping("word-spliter", new Fields("word"))
                .setNumTasks(100);
        
        submit("hot-words-topology", builder.createTopology(), true);
    }
    
    public static void submit(String name, StormTopology topology, boolean localMode){
        Config config = new Config();
        if(localMode){
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(name, config, topology);
        }else{
            config.setNumWorkers(10);
            config.setMaxSpoutPending(5000);
            try {
                StormSubmitter.submitTopology(name, config, topology);
            } catch (AlreadyAliveException | InvalidTopologyException ex) {
                LOG.error(ex.getMessage());
            }
        }
    }
}
