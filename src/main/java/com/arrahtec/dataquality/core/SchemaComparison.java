package com.arrahtec.dataquality.core;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class SchemaComparison {
	private String dbstr, dbstrSec;
	private String message;

	public SchemaComparison() {

	}

	@XmlElement
	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}

	public SchemaComparison(String dbstr, String dbstrSec) {
		super();
		this.dbstr = dbstr;
		this.dbstrSec = dbstrSec;
		compareSchema();
	}

	public void compareSchema() {
		try {
			SchemaComparisonServer compareSch = new SchemaComparisonServer(
					dbstr, dbstrSec);
			compareSch.compareResult();
			setMessage(compareSch.getMessage());
		} catch (Exception e) {
			e.getLocalizedMessage();
		}
	}
}
