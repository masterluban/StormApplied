package chapter02;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

public class EmailCounter extends BaseBasicBolt{

	private Map<String, Integer> counts;
	/**
	 * 
	 */
	private static final long serialVersionUID = 1776462068134967440L;

	
	
	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		super.prepare(stormConf, context);
		counts = new HashMap<>();
	}

	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		String email = tuple.getStringByField("email");
		counts.put(email, countFor(email)+1);
		printCounts();
		
	}
	
	private Integer countFor(String email){
		Integer count = counts.get(email);
		return count == null?0:count;
	}
	
	private void printCounts(){
		for (String email:counts.keySet()){
			System.out.println(String.format("%s has count of %s",email, counts.get(email)));
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// no output fields
		
	}

}
