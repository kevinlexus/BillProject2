package com.dic.app.mm;

import java.util.concurrent.Future;

import com.ric.dto.Result;


public interface ComprTbl {

	void comprTableByLsk(String table, String lsk, Integer backPeriod, Integer curPeriod, boolean isAllPeriods);

}