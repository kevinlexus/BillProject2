package com.dic.app.gis.service.st.builder;

import javax.xml.datatype.DatatypeConfigurationException;

import com.ric.cmn.excp.ErrorProcessAnswer;
import com.ric.cmn.excp.UnusableCode;
import com.ric.cmn.excp.WrongGetMethod;
import com.ric.cmn.excp.WrongParam;
import com.dic.bill.model.exs.Task;
import com.ric.cmn.excp.CantPrepSoap;
import com.ric.cmn.excp.CantSendSoap;

public interface HouseManagementAsyncBindingBuilders {

	void setUp(Task task) throws CantSendSoap, CantPrepSoap;
	void exportDeviceData(Task task) throws CantPrepSoap, WrongGetMethod, DatatypeConfigurationException;
	void exportDeviceDataAsk(Task task) throws CantPrepSoap, ErrorProcessAnswer, WrongGetMethod, UnusableCode;

	void exportHouseData(Task task) throws CantPrepSoap, WrongGetMethod;
	void exportHouseDataAsk(Task task) throws CantPrepSoap, WrongGetMethod, WrongParam, UnusableCode;
	void exportAccountData(Task task) throws CantPrepSoap, WrongGetMethod, CantSendSoap, WrongParam;
	void exportAccountDataAsk(Task task) throws CantPrepSoap, ErrorProcessAnswer, WrongGetMethod, WrongParam, CantSendSoap;
	void importAccountData(Task task) throws WrongGetMethod, CantPrepSoap, WrongParam, CantSendSoap, UnusableCode;
	void importAccountDataAsk(Task task) throws CantPrepSoap, WrongGetMethod, WrongParam, CantSendSoap, UnusableCode;
	void importHouseUOData(Task task) throws CantPrepSoap, WrongGetMethod;
	void importHouseUODataAsk(Task task) throws CantPrepSoap, WrongGetMethod;


	void exportBriefSupplyResourceContract(Task task) throws CantPrepSoap, WrongParam, WrongGetMethod, ErrorProcessAnswer, CantSendSoap;
	void exportBriefSupplyResourceContractAsk(Task task) throws CantPrepSoap, WrongParam, WrongGetMethod, ErrorProcessAnswer, CantSendSoap;

	void exportCaChData(Task task) throws CantPrepSoap, WrongGetMethod;
	void exportCaChDataAsk(Task task) throws CantPrepSoap, WrongGetMethod, WrongParam;
	void checkPeriodicHouseExp(Task task) throws WrongParam;
}
