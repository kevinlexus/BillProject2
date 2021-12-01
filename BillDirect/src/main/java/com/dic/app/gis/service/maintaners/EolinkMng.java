package com.dic.app.gis.service.maintaners;

import com.dic.app.gis.service.maintaners.impl.EolinkMngImpl;
import com.dic.bill.model.exs.Eolink;
import com.dic.bill.model.scott.Kart;

import java.util.List;
import java.util.Optional;

public interface EolinkMng {

	Eolink getEolinkByReuKulNdTp(String reu, String kul, String nd,
			String kw, String entry, String tp);
	void setChildActive(Eolink eolink, String tp, Integer status);

	List<Eolink> getLskEolByHouseEol(Integer eolHouseId, Integer eolUkId);

    Optional<Eolink> getEolinkByEolinkUpHierarchy(Eolink eolink, String cdTp);

    List<Eolink> getEolinkByEolinkDownHierarchy(Eolink eolink, String cdTp);

    List<Kart> getKartNotExistsInEolink(Integer eolHouseId, Integer eolUkId);

    EolinkMngImpl.EolinkParams getEolinkParamsOfKartMain(Kart kart);

    Eolink getEolinkRKC();

    List<Eolink> getEolinkUk();
}