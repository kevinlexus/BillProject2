package com.dic.app.service;

import com.dic.app.service.impl.GenPenMngImpl;
import com.dic.bill.model.scott.Kart;
import com.ric.cmn.excp.ErrorWhileChrgPen;

import java.math.BigDecimal;
import java.util.Date;
import java.util.Optional;

public interface GenPenMng {
    Optional<GenPenMngImpl.PenDTO> calcPen(BigDecimal summa, Integer mg, Kart kart, Date curDt) throws ErrorWhileChrgPen;
}
