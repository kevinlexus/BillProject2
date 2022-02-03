package com.dic.app.enums;

import lombok.Getter;

// статусы запроса по задолженности ГИС, для внутреннего использования
@Getter
public enum DebtSubRequestInnerStatuses {
    RECEIVED(0), // принято УК от ГИС
    SENT(1), // изменено и отправлено в ГИС
    ACCEPTED_BY_GIS(2), // принято ГИС
    REVOKED(3), // отозвано УК
    ACCEPTED_REVOKE_BY_GIS(4); // отзыв принят ГИС

    private final int id;

    DebtSubRequestInnerStatuses(int id) {
        this.id=id;
    }
}
