package chapter04;

import java.io.Serializable;

public class AuthorizationService implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = -725792536298605191L;

	 public boolean authorize(Order order) {
	    // In a real scenario, this would call an external service to verify the credit card number for the order.
	    return true;
	 }
	 
}
