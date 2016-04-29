package chapter04;

import java.io.Serializable;

public class NotificationService implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 7702595610208221663L;

	public void notifyOrderHasBeenProcessed(Order order) {
        // In a real scenario, this would notify any downstream systems that the order has been processed.
    }
	
}
