package com.dic.bill.mm;

import java.util.Date;
import java.util.Map;

import com.dic.bill.model.bs.Par;

public interface ParMng {

	boolean isExByCd(int rqn, String cd);
	Par getByCD(int rqn, String cd);
    void reloadParam(Map<String, Date> mapDate, Map<String, Boolean> mapParams);

}