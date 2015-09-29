package com.arrah.dataquality.core;

import java.sql.SQLException;
import java.util.Hashtable;

import com.arrah.framework.dataquality.Rdbms_conn;

public class ConnectionString {
		
	
    public ConnectionString() {
	}

	public static void Connection(String dbStr) throws SQLException {
		
	  
		 Hashtable<String, String> _fileParse;
		_fileParse = new Hashtable<String, String>();
		_fileParse.put("Database_Type", "");
		_fileParse.put("Database_Driver", "");
		_fileParse.put("Database_Protocol", "");
		_fileParse.put("Database_DSN", "");
		_fileParse.put("Database_User", "");
		_fileParse.put("Database_Passwd", "");
		_fileParse.put("Database_Catalog", "");
		_fileParse.put("Database_SchemaPattern", "");
		_fileParse.put("Database_TablePattern", "");
		_fileParse.put("Database_ColumnPattern", "");
		_fileParse.put("Database_TableType", "TABLE");

		String[] para = dbStr.split("/");
		_fileParse.put("Database_Type", para[0]);
		_fileParse.put("Database_Driver", para[1]);
		String[] para1 = para[2].split("~");
		para[2] = para1[0];
		StringBuilder sb = new StringBuilder(para[2]);
		for (int i = 1; i < para1.length; i++) {
			sb.append(":").append(para1[i]);
		}
		para[2]=sb.toString();
		
		String[] para2 = para[3].split("~");
		sb = new StringBuilder(para2[0]);
		
		if (para[0].equalsIgnoreCase("sql_server")) {
			 sb.append("//").append(para2[1]).append(":")
					.append(para2[2]).append(";databaseName=").append(para2[3]);
			 para[3] =sb.toString();
			
		} else if (para[0].equalsIgnoreCase("MS_ACCESS")) {
			para[3] = para[3];
		} else if (para[0].equalsIgnoreCase("db2")) {
			sb.append("//").append(para2[1]).append(":")
					.append(para2[2]).append("/").append(para2[3]);
			para[3] =sb.toString();
		} else if (para[0].equalsIgnoreCase("Oracle_native")) {
			sb.append(":").append(para2[2]).append(":")
					.append(para2[3]);
			para[3] =sb.toString();
		} else {
			sb.append("//").append(para2[1]).append(":")
					.append(para2[2]).append("/").append(para2[3]);
			para[3] = sb.toString();
			
			
		}

		_fileParse.put("Database_Protocol", para[2]);
		_fileParse.put("Database_DSN", para[3]);
		
		String[] para3 = para[4].split("~");
		para[4] = para3[0];
		
		sb=new StringBuilder(para[4]);
		for (int i = 1; i < para3.length; i++) {
			sb.append(" ").append(para3[i]);
		}
		para[4]=sb.toString();
		if(para[0].equalsIgnoreCase("hive"))
		{
			_fileParse.put("Database_User", "");
			_fileParse.put("Database_Passwd", "");
		}
		else
		{
		_fileParse.put("Database_User", para[4]);
		_fileParse.put("Database_Passwd", para[5]);
		}

		Rdbms_conn.init(_fileParse);
		Rdbms_conn.openConn();

		if (para[0].equalsIgnoreCase("MS_ACCESS")) {
			_fileParse.put("Database_DSN", para2[0]);
		} else {
			_fileParse.put("Database_DSN", para2[3]);
		}
	}
}
