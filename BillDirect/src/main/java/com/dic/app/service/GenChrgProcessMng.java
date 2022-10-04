package com.dic.app.service;

import com.dic.app.RequestConfigDirect;
import com.dic.bill.dto.LskChargeUsl;
import com.ric.cmn.excp.ErrorWhileChrg;

import java.util.List;

public interface GenChrgProcessMng {

    List<LskChargeUsl> genChrg(RequestConfigDirect reqConf, long klskId) throws ErrorWhileChrg;
}
