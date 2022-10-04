package com.dic.app.service;

import java.util.concurrent.Future;

import com.dic.bill.model.scott.SprGenItm;
import com.dic.bill.dto.CommonResult;

public interface GenThrMng {

	Future<CommonResult> doJob(Integer var, Long id, SprGenItm spr, double proc);

}
