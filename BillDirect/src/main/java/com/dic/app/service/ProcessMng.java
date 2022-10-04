package com.dic.app.service;

import com.dic.bill.dto.ChangesParam;
import com.dic.bill.model.scott.*;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.ric.cmn.excp.*;

import java.util.Date;
import java.util.concurrent.ExecutionException;

public interface ProcessMng {

    String processWebRequest(int tp, int debugLvl, Date genDt, House house, Vvod vvod, Ko ko, Org uk, Usl usl);


    int processChanges(ChangesParam changesParam) throws ExecutionException, InterruptedException, ErrorWhileGen, JsonProcessingException, WrongParamPeriod, WrongParam;
}
