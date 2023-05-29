package com.bartek.spark.parser;

import com.ximpleware.AutoPilot;
import com.ximpleware.VTDException;
import com.ximpleware.VTDNav;

public interface CustomFieldParser {
    Object parse(String field, String xpath, AutoPilot ap, VTDNav nav) throws VTDException;
}
