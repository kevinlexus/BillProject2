package com.dic.app.mm;

import com.dic.app.RequestConfigDirect;
import com.dic.bill.dto.ChrgCountAmountLocal;
import com.ric.dto.projection.SumMeterVol;
import com.dic.bill.dto.UslMeterDateVol;
import com.dic.bill.model.scott.Ko;
import com.dic.bill.model.scott.Nabor;
import com.dic.bill.model.scott.Usl;
import com.ric.cmn.excp.ErrorWhileChrg;
import com.ric.cmn.excp.WrongParam;

import java.util.Date;
import java.util.List;

public interface GenPart {
    void genVolPart(ChrgCountAmountLocal chrgCountAmountLocal,
                    RequestConfigDirect reqConf, int parVarCntKpr,
                    int parCapCalcKprTp, Ko ko, List<SumMeterVol> lstMeterVol, List<Usl> lstSelUsl,
                    List<UslMeterDateVol> lstDayMeterVol, Date curDt, int part, List<Nabor> lstNabor)
            throws ErrorWhileChrg, WrongParam;
}
