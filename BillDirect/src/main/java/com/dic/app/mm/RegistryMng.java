package com.dic.app.mm;

import com.dic.bill.dto.SumFinanceFlow;
import com.dic.bill.dto.UnloadPaymentParameter;
import com.ric.cmn.excp.ErrorWhileLoad;
import com.ric.cmn.excp.WrongParam;

import java.io.FileNotFoundException;
import java.io.IOException;

public interface RegistryMng {

    void genDebitForSberbank();
    int loadFileKartExt(Integer orgId, String fileName) throws FileNotFoundException, WrongParam, ErrorWhileLoad;

    int unloadPaymentFileKartExt(UnloadPaymentParameter unloadPaymentParameter) throws IOException, WrongParam;

    int loadFileMeterVal(String fileName, String codePage, boolean isSetPreviosVal) throws FileNotFoundException;
    int unloadFileMeterVal(String fileName, String codePage, String strUk) throws IOException;

    void loadApprovedKartExt(Integer orgId) throws WrongParam;

    void saveDBF(String tableName, String tableOutName) throws FileNotFoundException;

    StringBuilder getFlowFormatted(Long klskId, String periodBack);

    StringBuilder getChargeFormatted(Long klskId);
}
