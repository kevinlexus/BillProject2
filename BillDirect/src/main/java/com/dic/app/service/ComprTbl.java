package com.dic.app.service;


public interface ComprTbl {

	void comprTableByLsk(String table, String lsk, Integer backPeriod, Integer curPeriod, boolean isAllPeriods);

}