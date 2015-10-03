package com.arrah.dataquality.core;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class ExecuteQueryData {

	private ExecuteQuery execQuery;

	public ExecuteQueryData(){
		
		
	}
	@XmlElement
	public ExecuteQuery getExecQuery() {
		return execQuery;
	}

	public void setExecQuery(ExecuteQuery execQuery) {
		this.execQuery = execQuery;
	}
	
}
