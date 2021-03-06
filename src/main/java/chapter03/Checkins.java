package chapter03;

import java.io.IOException;
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

public class Checkins extends BaseRichSpout {

	/**
	 * 
	 */
	private static final long serialVersionUID = 8616399534500158581L;
	private SpoutOutputCollector outputCollector;
	private int nextEmitIndex;
	private List<String> checkins;

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.outputCollector = collector;
		this.nextEmitIndex = 0;
		try{
			checkins = IOUtils.readLines(ClassLoader.getSystemResourceAsStream("checkins.txt"),Charset.defaultCharset().name());
		}
		catch (IOException e){
			throw new RuntimeException(e);
		}
		
	}

	@Override
	public void nextTuple() {
		if (nextEmitIndex < checkins.size()){
			String checkin = checkins.get(nextEmitIndex);
			String[] parts = checkin.split(",");
			Long time = Long.valueOf(parts[0]);
			String address = parts[1];
			outputCollector.emit(new Values(time, address));
			nextEmitIndex = (nextEmitIndex+1);
		}
		//nextEmitIndex = (nextEmitIndex+1) % checkins.size();
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("time","address"));
		
	}

}
