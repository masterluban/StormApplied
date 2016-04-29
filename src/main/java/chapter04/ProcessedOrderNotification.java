package chapter04;

import java.util.Map;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

public class ProcessedOrderNotification extends BaseBasicBolt{

	/**
	 * 
	 */
	private static final long serialVersionUID = -1005248015315580971L;
	private NotificationService notificationService;
	
	@Override
	public void prepare(Map config,	TopologyContext context) {
		notificationService = new NotificationService();
	}
	
	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		Order order = (Order) tuple.getValueByField("order");
		notificationService.notifyOrderHasBeenProcessed(order);
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// This bolt does not emit anything. No output fields will be declared
		
	}

}
