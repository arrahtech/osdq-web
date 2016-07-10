package com.arrahtec.dataquality.core;
/***********************************************
 *     Copyright to          *
 *                                             *
 * Any part of code or file can be changed,    *
 * redistributed, modified with the copyright  *
 * information intact                          *
 *                                             *
 * Author$ :                       *
 *                                             *
 ***********************************************/

/* This file is POJO for setting and getting table, metadata and privileges 
 *
 */
//Adding Company resource
//Adding Company resource

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;


@XmlRootElement
public class Table_data {
	private Table table_data;
	
	public Table_data () {}

	@XmlElement
	public Table getTable_data() {
		return table_data;
	}

	public void setTable_data(Table table_data) {
		this.table_data = table_data;
	}

}

