package com.dic.app.service;

import com.ric.cmn.excp.ErrorWhileGen;

public interface MigrateMng {

	void migrateAll(String lskFrom, String lskTo, Integer dbgLvl) throws ErrorWhileGen;
	void migrateDeb(String lsk, Integer periodBack, Integer period, Integer dbgLvl);

}
