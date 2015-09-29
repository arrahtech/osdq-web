package com.arrah.dataquality.core;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class Table_Join {
	
	private Joins table_Join;
	private Merge table_merge;
	public Merge getTable_merge() {
		return table_merge;
	}

	public void setTable_merge(Merge table_merge) {
		this.table_merge = table_merge;
	}

	public Table_Join () {}

	@XmlElement
	public Joins getTable_Join() {
		return table_Join;
	}

	public void setTable_Join(Joins table_Join) {
		this.table_Join = table_Join;
	}

}