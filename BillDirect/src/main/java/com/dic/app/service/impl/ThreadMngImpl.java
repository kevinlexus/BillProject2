package com.dic.app.service.impl;

import com.dic.app.RequestConfigDirect;
import com.dic.app.service.ThreadMng;
import com.dic.bill.dto.CommonResult;
import com.dic.bill.dto.LskChargeUsl;
import com.ric.cmn.excp.ErrorWhileGen;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * Сервис создания потоков
 *
 * @author Lev
 * @version 1.00
 */
@Slf4j
@Service
public class ThreadMngImpl<T> implements ThreadMng<T> {

    @Autowired
    private ApplicationContext ctx;

    /**
     * Вызвать выполнение потоков распределения объемов/ начисления - новый метод
     *
     * @param reqConf кол-во потоков
     * @param rqn     номер запроса
     * @return результат начисления
     */
    @Override
    @Transactional(propagation = Propagation.REQUIRED, rollbackFor = Exception.class)
    public List<LskChargeUsl> invokeThreads(RequestConfigDirect reqConf, int rqn)
            throws ErrorWhileGen {
        if (reqConf.getLstItems().size() > 1) log.info("Будет создано {} потоков", reqConf.getCntThreads());
        List<CompletableFuture<CommonResult>> lst = new ArrayList<>();
        for (int i = 0; i < reqConf.getCntThreads(); i++) {
            // создать новый поток, передать информацию о % выполнения
            if (reqConf.getLstItems().size() > 1) log.info("********* Создан новый поток {}", reqConf.getTpName());
            ProcessAllMng processMng = ctx.getBean(ProcessAllMng.class);
            //log.info("********* Создан новый поток-2 tpName={}", reqConf.getTpName());
            CompletableFuture<CommonResult> ret = processMng.process(reqConf);
            //log.info("********* Создан новый поток-3 tpName={}", reqConf.getTpName());
            lst.add(ret);
            //log.info("********* Создан новый поток-4 tpName={}", reqConf.getTpName());
        }

        AtomicBoolean isError = new AtomicBoolean(false);
        // ждать потоки
        List<LskChargeUsl> resultLst = lst.stream().flatMap(t -> {
            try {
                return t.get().getLskChargeUsls().stream();
            } catch (InterruptedException | ExecutionException e) {
                isError.set(true);
                log.error("Ошибка во время выполнения потока");
            }
            return null;
        }).collect(Collectors.toList());

        if (isError.get()) {
            throw new ErrorWhileGen("Ошибка во время выполнения потока");
        } else {
            return resultLst;
        }
    }


}