package com.dic.app.gis.service.maintaners;

import com.dic.bill.RequestConfig;
import com.ric.cmn.excp.WrongGetMethod;
import com.ric.cmn.excp.WrongParam;
import com.ric.cmn.excp.CantPrepSoap;
import com.ric.cmn.excp.CantSendSoap;

public interface TaskControllers {

	void searchTask();
	RequestConfig getReqConfig();

}
