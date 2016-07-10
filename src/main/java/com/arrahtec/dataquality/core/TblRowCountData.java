package com.arrahtec.dataquality.core;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class TblRowCountData {
	private TblRowCount tableRowCntData;
	
	public TblRowCountData(){
		
	}

	@XmlElement
	public TblRowCount getTableRowCntData() {
		return tableRowCntData;
	}

	public void setTableRowCntData(TblRowCount tableRowCntData) {
		this.tableRowCntData = tableRowCntData;
	}
	
	
	
}
