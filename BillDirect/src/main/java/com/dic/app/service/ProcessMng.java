package com.dic.app.service;

import com.dic.app.service.impl.enums.ProcessTypes;
import com.dic.bill.dto.ChangesParam;
import com.dic.bill.model.scott.*;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.ric.cmn.excp.ErrorWhileGen;
import com.ric.cmn.excp.WrongParam;
import com.ric.cmn.excp.WrongParamPeriod;

import java.util.Date;
import java.util.concurrent.ExecutionException;

public interface ProcessMng {

    String processWebRequest(ProcessTypes tp, int debugLvl, Date genDt, House house, Vvod vvod, Ko ko, Org uk, Usl usl);


    int processChanges(ChangesParam changesParam) throws ExecutionException, InterruptedException, ErrorWhileGen, JsonProcessingException, WrongParamPeriod, WrongParam;
}
