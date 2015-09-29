package com.arrah.dataquality.core;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class Tableprivileges_data {
	private Table tableprivileges_data;

	public Tableprivileges_data() {
	};

	@XmlElement
	public Table getTableprivileges_data() {
		return tableprivileges_data;
	}

	public void setTableprivileges_data(Table _tableprivileges_data) {
		tableprivileges_data = _tableprivileges_data;
	}
}
