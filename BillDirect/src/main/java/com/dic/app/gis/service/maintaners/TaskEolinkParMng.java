package com.dic.app.gis.service.maintaners;

import java.util.Date;

import com.ric.cmn.excp.WrongGetMethod;
import com.dic.bill.model.exs.Task;

public interface TaskEolinkParMng {
	Double getDbl(Task task, String parCd) throws WrongGetMethod;
	String getStr(Task task, String parCd) throws WrongGetMethod;
	Date getDate(Task task, String parCd) throws WrongGetMethod;
	Boolean getBool(Task task, String parCd) throws WrongGetMethod;
//	void acceptPar(Task task);
}
