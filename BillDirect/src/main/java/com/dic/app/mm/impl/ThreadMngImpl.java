package com.dic.app.mm.impl;

import com.dic.app.RequestConfigDirect;
import com.dic.app.mm.ProcessMng;
import com.dic.app.mm.ThreadMng;
import com.ric.cmn.excp.ErrorWhileGen;
import com.ric.dto.CommonResult;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

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
    @PersistenceContext
    private EntityManager em;

    /**
     * Вызвать выполнение потоков распределения объемов/ начисления - новый метод
     *
     * @param reqConf - кол-во потоков
     * @param rqn     - номер запроса
     */
    @Override
    @Transactional(propagation = Propagation.REQUIRED, rollbackFor = Exception.class)
    public void invokeThreads(RequestConfigDirect reqConf, int rqn)
            throws ErrorWhileGen {
        if (reqConf.getLstItems().size() > 1) log.info("Будет создано {} потоков", reqConf.getCntThreads());
        List<CompletableFuture<CommonResult>> lst = new ArrayList<>();
        for (int i = 0; i < reqConf.getCntThreads(); i++) {
            // создать новый поток, передать информацию о % выполнения
            if (reqConf.getLstItems().size() > 1) log.info("********* Создан новый поток {}", reqConf.getTpName());
            ProcessMng processMng = ctx.getBean(ProcessMng.class);
            //log.info("********* Создан новый поток-2 tpName={}", reqConf.getTpName());
            CompletableFuture<CommonResult> ret = processMng.process(reqConf);
            //log.info("********* Создан новый поток-3 tpName={}", reqConf.getTpName());
            lst.add(ret);
            //log.info("********* Создан новый поток-4 tpName={}", reqConf.getTpName());
        }

        // ждать потоки
        lst.forEach(CompletableFuture::join);

    }


}