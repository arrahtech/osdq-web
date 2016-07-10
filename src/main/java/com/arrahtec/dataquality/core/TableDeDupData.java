package com.arrahtec.dataquality.core;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class TableDeDupData {
	private TableDeDup tableDupData;

	public TableDeDupData(){
		
	}

	@XmlElement
	public TableDeDup getTableDupData() {
		return tableDupData;
	}

	public void setTableDupData(TableDeDup tableDupData) {
		this.tableDupData = tableDupData;
	}
	
	
}
