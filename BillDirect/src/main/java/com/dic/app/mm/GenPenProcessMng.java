package com.dic.app.mm;

import com.dic.app.RequestConfigDirect;
import com.dic.bill.RequestConfig;
import com.dic.bill.dto.CalcStore;
import com.dic.bill.dto.CalcStoreLocal;
import com.dic.bill.model.scott.Kart;
import com.dic.bill.model.scott.Ko;
import com.ric.cmn.excp.ErrorWhileChrgPen;
import com.ric.dto.CommonResult;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.Date;
import java.util.concurrent.Future;

public interface GenPenProcessMng {

    void genDebitPen(RequestConfigDirect reqConf, boolean isCalcPen, long klskId) throws ErrorWhileChrgPen;

    @Transactional(
            propagation = Propagation.REQUIRED,
            isolation = Isolation.READ_COMMITTED, // читать только закомиченные данные, не ставить другое, не даст запустить поток!
            rollbackFor = Exception.class)
    void genDebitPenForTest(CalcStore calcStore, boolean isCalcPen, long klskId) throws ErrorWhileChrgPen;
}
