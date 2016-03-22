package chapter03;

import com.google.code.geocoder.model.LatLng;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class TimeIntervalExtractor extends BaseBasicBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 8211583373176725105L;

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		Long time = input.getLongByField("time");
		LatLng geocode = (LatLng) input.getValueByField("geocode");
		String city = input.getStringByField("city");
		Long timeInterval = time / (15*1000);
		collector.emit(new Values(timeInterval, geocode, city));
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("time-interval","geocode","city"));
		
	}

}
