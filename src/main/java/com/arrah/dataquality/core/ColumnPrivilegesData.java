package com.arrah.dataquality.core;


import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class ColumnPrivilegesData {

	private Columnprivileges column_privilegedata;

	public ColumnPrivilegesData () {};

	@XmlElement
	public Columnprivileges getColumn_Priviligesdata() {
		return column_privilegedata;
	}
	public void  setColumn_Priviligesdata(Columnprivileges _column_privilegedata) {
		column_privilegedata = _column_privilegedata;
	}
}
