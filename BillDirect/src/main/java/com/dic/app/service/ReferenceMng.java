package com.dic.app.service;

import com.dic.bill.dto.UslOrg;
import com.dic.bill.model.scott.Kart;

public interface ReferenceMng {

	UslOrg getUslOrgRedirect(String uslId, Integer orgId, Kart kart, Integer tp);

}
