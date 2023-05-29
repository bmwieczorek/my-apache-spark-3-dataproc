package com.bartek.spark.parser;

import com.ximpleware.AutoPilot;
import com.ximpleware.VTDException;
import com.ximpleware.VTDNav;

public class SumValuesParser implements CustomFieldParser {

    @Override
    public Object parse(String field, String xpath, AutoPilot ap, VTDNav nav) throws VTDException {
        int sum = 0;
        if (xpath.startsWith("@") || xpath.contains("/@")) {
            int i;
            while ((i = ap.evalXPath()) > 0) {
                String attrName = nav.toString(i);
                int attrIdx = nav.getAttrVal(attrName);
                if (attrIdx != -1) {
                    String value = nav.toString(attrIdx);
                    int component = Integer.parseInt(value);
                    sum = sum + component;
                }
            }
        } else {
            while ((ap.evalXPath()) != -1) {
                long attrPosition = nav.getContentFragment();
                if (attrPosition != -1) {
                    String value = nav.toString(nav.getText());
                    int component = Integer.parseInt(value);
                    sum = sum + component;
                }
            }
        }
        ap.resetXPath();
        return sum;
    }
}
