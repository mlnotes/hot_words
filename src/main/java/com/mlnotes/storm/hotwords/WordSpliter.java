package com.mlnotes.storm.hotwords;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.huaban.analysis.jieba.JiebaSegmenter;
import com.huaban.analysis.jieba.JiebaSegmenter.SegMode;
import com.huaban.analysis.jieba.SegToken;
import com.huaban.analysis.jieba.WordDictionary;
import java.io.File;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author zhf
 * @date 2014-3-5 20:56:26
 */
public class WordSpliter extends BaseBasicBolt{
   public static Logger LOG = LoggerFactory.getLogger(App.class);
    
    private JiebaSegmenter segmenter;
    
    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        WordDictionary.getInstance().init(new File(Constants.JIEBA_CONF));
        segmenter = new JiebaSegmenter();
    }
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        String line = input.getString(0);
        LOG.info("new line: " + line);
        
        List<SegToken> tokens = segmenter.process(line, SegMode.INDEX);
        for(SegToken t : tokens){
            String word = t.word;
            LOG.info("word: " + word);
            if(word.length() > 1){
                collector.emit(new Values(word));
            }
        }
    }
}
