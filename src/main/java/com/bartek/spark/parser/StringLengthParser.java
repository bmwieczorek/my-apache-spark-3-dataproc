package com.bartek.spark.parser;

import com.ximpleware.AutoPilot;
import com.ximpleware.VTDNav;
import com.ximpleware.XPathParseException;

public class StringLengthParser implements CustomFieldParser {

    @Override
    public Object parse(String field, String xpath, AutoPilot ap, VTDNav nav) {
        try {
            ap.selectXPath(xpath);
        } catch (XPathParseException e) {
            e.printStackTrace();
        }
        String s = ap.evalXPathToString();
        return s.length();
    }
}
