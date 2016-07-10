package com.arrahtec.dataquality.core;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;


@XmlRootElement
public class Row {
		@XmlElement
                public String[] data;
        }


