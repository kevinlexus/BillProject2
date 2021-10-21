package com.dic.app.mm.impl;

import com.dic.app.mm.ChangeMng;
import com.dic.bill.dto.ChangesParam;

public class ChangeMngImpl implements ChangeMng {


    @Override
    public int genChanges(ChangesParam changesParam) {
        // todo сделать многопоточку по объектам
        changesParam.getSelObjList(); //todo

        return 0;
    }

}
