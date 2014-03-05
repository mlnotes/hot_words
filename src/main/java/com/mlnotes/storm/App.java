package com.mlnotes.storm;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import java.util.logging.Level;
import java.util.logging.Logger;

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
        //LocalCluster cluster = new LocalCluster();
            //cluster.submitTopology("word-count-topology", config, builder.createTopology());
        
        config.setNumWorkers(10);
        config.setMaxSpoutPending(5000);
        try {
            StormSubmitter.submitTopology("word-count-topology", config, builder.createTopology());
        } catch (AlreadyAliveException ex) {
            Logger.getLogger(App.class.getName()).log(Level.SEVERE, null, ex);
        } catch (InvalidTopologyException ex) {
            Logger.getLogger(App.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
