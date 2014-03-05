package com.mlnotes.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("word-reader", new WordReader());
        builder.setBolt("word-spliter", new WordSpliter())
                .shuffleGrouping("word-reader");
        builder.setBolt("word-counter", new WordCounter())
                .fieldsGrouping("word-spliter", new Fields("word"));
    
        // configuration
        Config config = new Config();
        // TODO set config
        
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("word-count-topoloty", config, builder.createTopology());
        
    }
}
