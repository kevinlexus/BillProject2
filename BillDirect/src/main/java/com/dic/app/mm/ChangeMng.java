package com.dic.app.mm;

import com.dic.bill.dto.ChangesParam;
import com.dic.bill.dto.LskCharge;
import com.dic.bill.dto.LskChargeUsl;
import com.dic.bill.dto.ResultChange;

import java.util.List;
import java.util.Map;

public interface ChangeMng {

    List<ResultChange> genChanges(ChangesParam changesParam, Long klskId, Map<String, Map<String, List<LskChargeUsl>>> value);
}
