package com.arrah.dataquality.core;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class TableMetaData_data {
	private Table tableMetaData;

	public TableMetaData_data() {
	}

	@XmlElement
	public Table getTableMetaData() {
		return tableMetaData;
	}

	public void setTableMetaData(Table tableMetaData) {
		this.tableMetaData = tableMetaData;
	};

}
