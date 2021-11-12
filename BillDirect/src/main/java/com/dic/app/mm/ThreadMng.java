package com.dic.app.mm;

import com.dic.app.RequestConfigDirect;
import com.dic.bill.dto.LskChargeUsl;
import com.ric.cmn.excp.ErrorWhileGen;

import java.util.List;

public interface ThreadMng<T> {

    List<LskChargeUsl> invokeThreads(RequestConfigDirect reqConf, int rqn)
            throws ErrorWhileGen;

}
