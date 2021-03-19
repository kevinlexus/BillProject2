package com.dic.app.mm;

import java.util.Date;
import java.util.List;

import com.dic.bill.model.scott.SprGenItm;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

public interface ExecMng {

	void updateSprGenItem(List<SprGenItm> lst);
	Integer execProc(Integer var, Long id, Integer sel);
	//void setGenDate();
	void stateBase(int state);
	void setMenuElemPercent(SprGenItm spr, double proc);
	void clearPercent();

    @Transactional(propagation = Propagation.REQUIRES_NEW, rollbackFor = Exception.class)
    void setMenuElemDuration(SprGenItm spr, double duration);

    void setMenuElemState(SprGenItm spr, String state);
	void setMenuElemDt1(SprGenItm spr, Date dt1);
	void setMenuElemDt2(SprGenItm spr, Date dt2);

}
