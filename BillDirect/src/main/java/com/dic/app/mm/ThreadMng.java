package com.dic.app.mm;

import com.dic.app.RequestConfigDirect;
import com.ric.cmn.excp.ErrorWhileChrg;
import com.ric.cmn.excp.ErrorWhileGen;
import com.ric.cmn.excp.WrongParam;

import java.util.List;
import java.util.concurrent.ExecutionException;

public interface ThreadMng<T> {

    void invokeThreads(RequestConfigDirect reqConf, int rqn)
            throws ErrorWhileGen;

}
