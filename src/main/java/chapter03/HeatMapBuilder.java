package chapter03;

import backtype.storm.Config;
import backtype.storm.Constants;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.google.code.geocoder.model.LatLng;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class HeatMapBuilder extends BaseBasicBolt{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1945457326210736753L;
	private Map<Long,List<LatLng>> heatMaps;
	
	
	
	
	@Override
	public Map<String, Object> getComponentConfiguration() {
		Config conf = new Config();
		conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS,60);
		return conf;
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		super.prepare(stormConf, context);
		heatMaps = new HashMap<>();
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		if (isTickTuple(input)){
			emitHeatmap(collector);
		}
		else {
			Long time = input.getLongByField("time");
			LatLng geocode = (LatLng) input.getValueByField("address");
			Long timeInterval = selectTimeInterval(time);
			List<LatLng> checkins = getCheckinsForInterval(timeInterval); 
			checkins.add(geocode);
		}
	}

	private void emitHeatmap(BasicOutputCollector collector) {
		Long now = System.currentTimeMillis();
		Long emitUpToTimeInterval = selectTimeInterval(now);
		Set<Long> timeIntervalsAvailable = heatMaps.keySet();
		for (Long timeInterval:timeIntervalsAvailable){
			if (timeInterval <= emitUpToTimeInterval){
				List<LatLng> hotzones = heatMaps.remove(timeInterval);
				collector.emit(new Values (timeInterval, hotzones));
			}
		}
		
	}

	private boolean isTickTuple(Tuple input) {
		String sourceComponent = input.getSourceComponent();
		String sourceStreamId = input.getSourceStreamId();
		return sourceComponent.equals(Constants.SYSTEM_COMPONENT_ID)
				&& sourceStreamId.equals(Constants.SYSTEM_TICK_STREAM_ID);
		
	}

	private List<LatLng> getCheckinsForInterval(Long timeInterval) {
		List<LatLng> hotzones = heatMaps.get(timeInterval);
		if (hotzones == null){
			hotzones = new ArrayList<LatLng>();
			heatMaps.put(timeInterval, hotzones);
		}
		return hotzones;
	}

	private Long selectTimeInterval(Long time) {
		return time / (15 * 1000);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("time-interval","hotzones"));
		
	}

}
