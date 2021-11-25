package com.dic.app.gis.service.maintaners;

import com.dic.bill.dto.OrgDTO;
import com.dic.bill.model.exs.Eolink;

/**
 * Интерфейс сервиса организации
 * @author Leo
 *
 */
public interface OrgMng {

	OrgDTO getOrgDTO(Integer appTp);

    OrgDTO getOrgDTO(Eolink uk);
}
