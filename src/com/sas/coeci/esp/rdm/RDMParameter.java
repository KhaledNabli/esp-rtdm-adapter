package com.sas.coeci.esp.rdm;

public class RDMParameter<T> {

	public enum Datatype {
		String, Double, Long, Boolean, DateTime
	}
	
	private String name;
	private Datatype type;
	private T value;

	/**
	 * 
	 * @param name
	 *            Parameter Name
	 * @param type
	 *            Parameter Type, Supported: String, Double, Date, Boolean,
	 *            Integer - No Lists!
	 * @param value
	 *            Serialized Value
	 */
	public RDMParameter(String name, Datatype type, T value) {
		this.name = name;
		this.type = type;
		this.value = value;
	}
	

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Datatype getType() {
		return type;
	}

	public void setType(Datatype type) {
		this.type = type;
	}

	public T getValue() {
		return value;
	}

	public void setValue(T value) {
		this.value = value;
	}

}
