package chapter03;

import java.util.Map;

import com.google.code.geocoder.Geocoder;
import com.google.code.geocoder.GeocoderRequestBuilder;
import com.google.code.geocoder.model.GeocodeResponse;
import com.google.code.geocoder.model.GeocoderAddressComponent;
import com.google.code.geocoder.model.GeocoderRequest;
import com.google.code.geocoder.model.GeocoderResult;
import com.google.code.geocoder.model.GeocoderStatus;
import com.google.code.geocoder.model.LatLng;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class GeoCodeLookup extends BaseBasicBolt{

	/**
	 * 
	 */
	private static final long serialVersionUID = -1605495486694518703L;
	private Geocoder geocoder;
	private int counter = 0;
	
	
	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		super.prepare(stormConf, context);
		geocoder = new Geocoder();
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		super.cleanup();
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		Long time = input.getLongByField("time");
		String address = input.getStringByField("address");
		
		GeocoderRequest request = new GeocoderRequestBuilder()
				.setAddress(address)
				.setLanguage("en")
				.getGeocoderRequest();
		GeocodeResponse response = geocoder.geocode(request);
		GeocoderStatus status = response.getStatus();
		System.out.println("Geocoder status " + status.toString());
		if (GeocoderStatus.OK.equals(status)){
			GeocoderResult firstResult = response.getResults().get(0);
			LatLng latLng = firstResult.getGeometry().getLocation();
			String city = extractCity(firstResult);
			counter++;
			System.out.println("Emitiram ..." + counter + "latlng="  + latLng.toString() + " city=" + city);
			
			collector.emit(new Values(time,latLng,city));
		}
		
		
	}

	private String extractCity(GeocoderResult firstResult) {
		for (GeocoderAddressComponent component: firstResult.getAddressComponents()){
			if (component.getTypes().contains("locality")){
				return component.getLongName();
			}
		}
		return "";
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("time","geocode","city"));
		
	}

	
}
