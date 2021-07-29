package com.dic.bill.mm;

import com.ric.cmn.excp.WrongGetMethod;
import com.ric.cmn.excp.WrongParam;
import com.ric.dto.ListKoAddress;

import java.math.BigDecimal;
import java.util.Date;

public interface ObjParMng {
    BigDecimal getBd(long klskId, String cd) throws WrongParam, WrongGetMethod;

    String getStr(long klskId, String cd) throws WrongParam, WrongGetMethod;

    Boolean getBool(long klskId, String cd) throws WrongParam, WrongGetMethod;

    void setBool(long klskId, String cd, boolean val) throws WrongParam, WrongGetMethod;

    void setBoolNewTransaction(long klskId, String cd, boolean val) throws WrongParam, WrongGetMethod;

    Date getDate(long klskId, String cd) throws WrongParam, WrongGetMethod;

    ListKoAddress getListKoAddressByObjPar(String cd, Long userId);
}
