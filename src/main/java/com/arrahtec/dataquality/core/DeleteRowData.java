package com.arrahtec.dataquality.core;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name="Delete_Info")
public class DeleteRowData {
	private DeleteRow deleteRow;

	public DeleteRowData(){
	}
	@XmlElement(name="Delete_Message")
	public DeleteRow getDelete_row() {
		return deleteRow;
	}

	public void setDelete_row(DeleteRow delete_row) {
		this.deleteRow = delete_row;
	}
	
	
}
