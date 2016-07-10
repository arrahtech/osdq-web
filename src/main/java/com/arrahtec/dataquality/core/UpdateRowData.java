package com.arrahtec.dataquality.core;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class UpdateRowData {

	private UpdateRow updateRow;

	@XmlElement
	public UpdateRow getUpdateRow() {
		return updateRow;
	}

	public void setUpdateRow(UpdateRow updateRow) {
		this.updateRow = updateRow;
	}
	
}
