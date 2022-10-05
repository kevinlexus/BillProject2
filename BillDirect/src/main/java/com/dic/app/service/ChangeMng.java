package com.dic.app.service;

import com.dic.bill.dto.*;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Map;

public interface ChangeMng {

    List<ResultChange> genChangesProc(ChangesParam changesParam, Long klskId, Map<String, Map<String, List<LskChargeUsl>>> value);

    @Transactional(propagation = Propagation.REQUIRED, rollbackFor = Exception.class)
    List<ResultChange> genChangesAbs(ChangesParam changesParam, Long klskId, Map<String, Map<String, List<LskNabor>>> chargeByPeriod);
}
