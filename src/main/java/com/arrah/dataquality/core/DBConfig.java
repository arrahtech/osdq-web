package com.arrah.dataquality.core;

import java.util.ResourceBundle;

public class DBConfig {
	
	private static ResourceBundle bundle;
	static {
		bundle = ResourceBundle.getBundle("config");
	}
	
	public static String dbProperties() {
		return bundle.getString("db.connstring");
	}
}
