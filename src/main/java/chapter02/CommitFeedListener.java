package chapter02;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;

import org.apache.storm.shade.org.apache.commons.io.IOUtils;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class CommitFeedListener extends BaseRichSpout {

	/**
	 * 
	 */
	private static final long serialVersionUID = 5953695255363118457L;
	private SpoutOutputCollector outputCollector;
	private List<String> commits;
	
	@Override
	public void nextTuple() {
		for (String commit:commits){
			outputCollector.emit(new Values(commit));
		}
		
	}

	@Override
	public void open(Map configMap, TopologyContext context, SpoutOutputCollector outputCollector) {
		this.outputCollector = outputCollector;
		
		try{
			InputStream input = ClassLoader.getSystemResourceAsStream("changelog.txt");
			System.out.println("input="+input.toString());
			commits = IOUtils.readLines(input,Charset.defaultCharset().name());
		}
		catch (IOException ex){
			throw new RuntimeException(ex);
		}
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("commit"));
		
	}

}
