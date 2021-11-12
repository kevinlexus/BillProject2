package com.dic.app.mm;

import com.dic.app.RequestConfigDirect;
import com.ric.cmn.excp.ErrorWhileChrgPen;

public interface GenPenProcessMng {

    void genDebitPen(RequestConfigDirect reqConf, boolean isCalcPen, long klskId) throws ErrorWhileChrgPen;

    void genDebitPenForTest(RequestConfigDirect reqConf, boolean isCalcPen, long klskId) throws ErrorWhileChrgPen;
}
