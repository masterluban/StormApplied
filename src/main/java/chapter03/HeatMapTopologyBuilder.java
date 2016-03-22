package chapter03;

import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class HeatMapTopologyBuilder {

	public static StormTopology build(){
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("checkins", new Checkins(),4);
		builder.setBolt("geocode-lookup", new GeoCodeLookup(),8).setNumTasks(64).shuffleGrouping("checkins");
		builder.setBolt("time-interval-extractor",new TimeIntervalExtractor(),4).shuffleGrouping("geocode-lookup");
		builder.setBolt("heatmap-builder", new HeatMapBuilder(),4).fieldsGrouping("time-interval-extractor", new Fields("time-interval","city"));
		builder.setBolt("persistor", new Persistor(),1).setNumTasks(4).shuffleGrouping("heatmap-builder");
		return builder.createTopology();
	}
}
