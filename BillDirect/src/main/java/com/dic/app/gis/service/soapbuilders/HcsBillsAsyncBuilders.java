package com.dic.app.gis.service.soapbuilders;

import javax.xml.datatype.DatatypeConfigurationException;

import com.ric.cmn.excp.WrongGetMethod;
import com.ric.cmn.excp.WrongParam;
import com.dic.bill.model.exs.Task;
import com.ric.cmn.excp.CantPrepSoap;
import com.ric.cmn.excp.CantSendSoap;

import java.text.ParseException;

public interface HcsBillsAsyncBuilders {

	//void setUp(Task task) throws CantSendSoap, CantPrepSoap;
	ru.gosuslugi.dom.schema.integration.bills.GetStateResult getState2(Task task);
	void exportNotificationsOfOrderExecution(Task task) throws WrongGetMethod, DatatypeConfigurationException, CantPrepSoap, WrongParam, CantSendSoap;
	void exportNotificationsOfOrderExecutionAsk(Task task) throws CantPrepSoap, WrongGetMethod, WrongParam, com.ric.cmn.excp.ErrorWhileDist, CantSendSoap;
	void exportPaymentDocumentData(Task task) throws CantPrepSoap, WrongGetMethod, CantSendSoap;
	void exportPaymentDocumentDataAsk(Task task) throws CantPrepSoap, WrongGetMethod, CantSendSoap;
	void importPaymentDocumentData(Task task) throws WrongGetMethod, DatatypeConfigurationException, CantPrepSoap, WrongParam, ParseException, CantSendSoap;
	void importPaymentDocumentDataAsk(Task task) throws CantSendSoap, CantPrepSoap, WrongGetMethod;
	void checkPeriodicImpExpPd(Task task) throws WrongParam;
}