package chapter04;

import java.io.Serializable;

public class Order implements Serializable {

	private static final long serialVersionUID = -2113878390116973141L;

	private long id;
	private long customerId;
	private long creditCardNumber;
	private String creditCardExpiration;
	private int creditCardCode;
	private double chargeAmount;

	public Order(long id, long customerId, long creditCardNumber, String creditCardExpiration, int creditCardCode,
			double chargeAmount) {
		this.id = id;
		this.customerId = customerId;
		this.creditCardNumber = creditCardNumber;
		this.creditCardExpiration = creditCardExpiration;
		this.creditCardCode = creditCardCode;
		this.chargeAmount = chargeAmount;
	}

	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}

	public long getCustomerId() {
		return customerId;
	}

	public void setCustomerId(long customerId) {
		this.customerId = customerId;
	}

	public long getCreditCardNumber() {
		return creditCardNumber;
	}

	public void setCreditCardNumber(long creditCardNumber) {
		this.creditCardNumber = creditCardNumber;
	}

	public String getCreditCardExpiration() {
		return creditCardExpiration;
	}

	public void setCreditCardExpiration(String creditCardExpiration) {
		this.creditCardExpiration = creditCardExpiration;
	}

	public int getCreditCardCode() {
		return creditCardCode;
	}

	public void setCreditCardCode(int creditCardCode) {
		this.creditCardCode = creditCardCode;
	}

	public double getChargeAmount() {
		return chargeAmount;
	}

	public void setChargeAmount(double chargeAmount) {
		this.chargeAmount = chargeAmount;
	}
	
	
}
