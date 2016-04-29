package chapter04;

import java.util.Map;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class AuthorizeCreditCard extends BaseBasicBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = -6232847268174517462L;
	private AuthorizationService authorizationService;
	private OrderDao orderDao;
	
	
	
	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		orderDao = new OrderDao();
		authorizationService = new AuthorizationService();
	}

	@Override
	public void execute(Tuple tuple, BasicOutputCollector outputCollector) {
		Order order = (Order) tuple.getValueByField("order");
		if (orderDao.isNotReadyToShip(order)) {
			boolean isAuthorized = authorizationService.authorize(order);
			if (isAuthorized) {
				orderDao.updateStatusToReadyToShip(order);
			} else {
				orderDao.updateStatusToDenied(order);
			}
		}
		outputCollector.emit(new Values(order));
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("order"));
		
	}

}
