package com.dic.app.mm;

import com.dic.app.RequestConfigDirect;
import com.dic.bill.dto.ChangesParam;
import com.dic.bill.dto.LskChargeUsl;
import com.dic.bill.model.scott.*;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.ric.cmn.excp.*;
import com.dic.bill.dto.CommonResult;

import java.util.Date;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public interface ProcessMng {

    String processWebRequest(int tp, int debugLvl, Date genDt, House house, Vvod vvod, Ko ko, Org uk, Usl usl);


    int processChanges(ChangesParam changesParam) throws ExecutionException, InterruptedException, ErrorWhileGen, JsonProcessingException, WrongParamPeriod, WrongParam;
}
