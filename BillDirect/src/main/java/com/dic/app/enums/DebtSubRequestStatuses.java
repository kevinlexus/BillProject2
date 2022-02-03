package com.dic.app.enums;

import com.ric.cmn.excp.WrongParam;
import lombok.Getter;

import java.util.Arrays;
import java.util.List;

// статусы запроса по задолженности в ГИС
@Getter
public enum DebtSubRequestStatuses {
    SENT(0, "Sent"), // отправлено в УК
    PROCESSED(1, "Processed"), // обработка  (чья???)
    REVOKED(2, "Revoked"); // отозвано отправившей организацией

    private final int id;
    private final String name;

    DebtSubRequestStatuses(int id, String name) {
        this.id=id;
        this.name = name;
    }

    public static DebtSubRequestStatuses getByName(String name) throws WrongParam {
        DebtSubRequestStatuses[] list = DebtSubRequestStatuses.values();
        for (DebtSubRequestStatuses status : list) {
            if (status.getName().equals(name))
                return status;
        }
        throw new WrongParam("Не найден DebtSubRequestStatuses.name = "+name);
    }

}
