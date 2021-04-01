package com.dic.bill.mm;

import com.dic.bill.mm.impl.SprParamMngImpl;
import com.ric.cmn.excp.WrongParam;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.Date;

public interface SprParamMng {

	Double getN1(String cd) throws WrongParam;

	String getS1(String cd) throws WrongParam;

	Date getD1(String cd) throws WrongParam;

	Boolean getBool(String cd) throws WrongParam;

    @Transactional(propagation = Propagation.REQUIRED, rollbackFor = Exception.class)
	SprParamMngImpl.StavPen getStavPen();
}