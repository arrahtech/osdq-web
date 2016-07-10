package com.arrahtec.dataquality.core;

import com.arrah.framework.ReportTableModel;

import java.util.ArrayList;
import java.util.Arrays;

public class FillCheck {
    
    // Constructor
    public FillCheck() {
                    // Do nothing

    }
    
    // Count null or empty object
    public static int getEmptyNo(Object[] row) {
                    int emptyNo=0;
                    if ( row == null ) return emptyNo;
                    
                   for ( int i=0; i < row.length; i++)
                                    if ( row[i] == null || "".equals(row[i].toString()) )
                                                    emptyNo++;
                    
                    return emptyNo;
    }

    public static int getEmptyNo(ArrayList<Object> row) {
                    int emptyNo=0;
                    if ( row == null ) return emptyNo;
                    emptyNo = getEmptyNo(row.toArray());
                    return emptyNo;
    }
    
    public static int[] getEmptyCount(Object[][] dataset) {
                    if (dataset == null) return null;
                    int[] emptyCount = new int[dataset[0].length];
                    Arrays.fill(emptyCount, 0);
                    
                    for (int i=0; i < dataset.length; i++) {
                                    int emptyNo = getEmptyNo(dataset[i]);
                                    emptyCount[emptyNo] = emptyCount[emptyNo] + 1;
                    }
                    
                    return emptyCount;
    }
    
    public static int[] getEmptyCount(ReportTableModel rtm) {
                    if (rtm== null) return null;
                    int rowc = rtm.getModel().getRowCount();
                    int colc = rtm.getModel().getColumnCount();
                    
                    int[] emptyCount = new int[colc];
                    
                    for (int i=0; i <rowc; i++ ) {
                                    Object[] row = rtm.getRow(i);
                                    int emptyNo = getEmptyNo(row);
                                    emptyCount[emptyNo] = emptyCount[emptyNo] + 1;
                    }
                    
                    return emptyCount;
    }
    
    
}
