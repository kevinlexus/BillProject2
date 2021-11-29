package com.dic.app.gis.service.maintaners;

import com.dic.bill.model.exs.Eolink;

public interface EolinkToEolinkMng {

	public boolean saveParentChild(Eolink parent, Eolink child, String tp);

}