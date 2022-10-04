package com.dic.app.service;

import com.dic.app.RequestConfigDirect;
import com.dic.bill.dto.CalcStoreLocal;
import com.dic.bill.model.scott.Kart;
import com.ric.cmn.excp.ErrorWhileChrgPen;

public interface DebitByLskThrMng {
    void genDebPen(Kart kart, RequestConfigDirect requestConfigDirect,
                   CalcStoreLocal localStore) throws ErrorWhileChrgPen;

}
