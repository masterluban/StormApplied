package chapter03;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.code.geocoder.model.LatLng;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import redis.clients.jedis.Jedis;


public class Persistor extends BaseBasicBolt{

	/**
	 * 
	 */
	private static final long serialVersionUID = -3379869387224041109L;
	private final Logger logger = LoggerFactory.getLogger(Persistor.class);
	private Jedis jedis;
	private ObjectMapper objectMapper;
	
	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		Long timeInterval = input.getLongByField("time-interval");
		List<LatLng> hz = (List<LatLng>) input.getValueByField("hotzones");
		List<String> hotzones = asListOfString(hz);
		
		try{
			String key = "checkins-" + timeInterval;
		    String value = objectMapper.writeValueAsString(hotzones);
		    jedis.set(key, value);
		}
		catch (Exception e){
			logger.error("Error persisting for time: " + timeInterval, e);
		}
		
	}

	private List<String> asListOfString(List<LatLng> hotzones) {
		List<String> hotzonesStandard = new ArrayList<String>(hotzones.size());
		for (LatLng getCoordinate:hotzones){
			hotzonesStandard.add(getCoordinate.toUrlValue());
		}
		return hotzonesStandard;
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		jedis = new Jedis("localhost");
		objectMapper = new ObjectMapper();
	}

	@Override
	public void cleanup() {
		if (jedis.isConnected())
			jedis.quit();
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		
	}

}
