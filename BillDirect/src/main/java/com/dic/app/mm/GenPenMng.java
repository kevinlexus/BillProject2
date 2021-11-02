package com.dic.app.mm;

import com.dic.app.mm.impl.GenPenMngImpl;
import com.dic.bill.dto.CalcStore;
import com.dic.bill.model.scott.Kart;
import com.ric.cmn.excp.ErrorWhileChrg;
import com.ric.cmn.excp.ErrorWhileChrgPen;

import java.math.BigDecimal;
import java.util.Date;
import java.util.Optional;

public interface GenPenMng {
    Optional<GenPenMngImpl.PenDTO> calcPen(BigDecimal summa, Integer mg, Kart kart, Date curDt) throws ErrorWhileChrgPen;
}
