package chapter03;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;

public class LocalTopologyRunner {

	public static void main(String[] args){
		Config config = new Config();
		StormTopology topology = HeatMapTopologyBuilder.build();
		
		LocalCluster localCluster = new LocalCluster();
		localCluster.submitTopology("local-heatmap", config, topology);
	}
}
