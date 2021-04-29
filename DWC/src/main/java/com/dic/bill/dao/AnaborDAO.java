package com.dic.bill.dao;

import java.util.List;

import com.dic.bill.model.scott.Anabor;
import com.dic.bill.model.scott.Kart;


public interface AnaborDAO {

	List<Anabor> getAll();
	List<Anabor> getByLsk(String lsk);
	List<Anabor> getByLskPeriod(String lsk, Integer period);
	List<Kart> getAfterLsk(String firstLsk);

}
