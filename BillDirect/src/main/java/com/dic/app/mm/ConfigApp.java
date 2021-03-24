package com.dic.app.mm;

import com.dic.bill.Lock;
import com.dic.bill.dto.SprPenKey;
import com.dic.bill.model.scott.SprPen;
import com.dic.bill.model.scott.Stavr;
import com.dic.bill.model.scott.Tuser;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.Date;
import java.util.List;
import java.util.Map;

public interface ConfigApp {

	Integer getProgress();

    String getPeriod();

	Tuser getCurUser();

    String getPeriodNext();

	String getPeriodBack();

	Date getCurDt1();

	Date getCurDt2();

	Lock getLock();

	int incNextReqNum();

	void setProgress(Integer progress);

	void incProgress();

	Map<SprPenKey, SprPen> getMapSprPen();

	List<Stavr> getLstStavr();


}