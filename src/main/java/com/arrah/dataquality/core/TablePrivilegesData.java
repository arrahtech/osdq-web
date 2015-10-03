package com.arrah.dataquality.core;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class TablePrivilegesData {
	private Table tablePrivilegesData;

	public TablePrivilegesData() {
	};

	@XmlElement
	public Table getTablePrivilegesData() {
		return tablePrivilegesData;
	}

	public void setTablePrivilegesData(Table tablePrivilegesData) {
		this.tablePrivilegesData = tablePrivilegesData;
	}
}
