package com.dic.app.mm;

import com.dic.app.RequestConfigDirect;
import com.dic.bill.dto.CalcStore;
import com.ric.cmn.excp.ErrorWhileChrgPen;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

public interface GenPenProcessMng {

    void genDebitPen(RequestConfigDirect reqConf, boolean isCalcPen, long klskId) throws ErrorWhileChrgPen;

    @Transactional(
            propagation = Propagation.REQUIRED,
            isolation = Isolation.READ_COMMITTED, // читать только закомиченные данные, не ставить другое, не даст запустить поток!
            rollbackFor = Exception.class)
    void genDebitPenForTest(CalcStore calcStore, boolean isCalcPen, long klskId) throws ErrorWhileChrgPen;
}
