package chapter02;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class EmailExtractor extends BaseBasicBolt{

	/**
	 * 
	 */
	private static final long serialVersionUID = 4460161409960364077L;

	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		String commit = tuple.getStringByField("commit");
		String[] parts = commit.split(" ");
		collector.emit(new Values(parts[1]));
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("email"));
		
	}

}
