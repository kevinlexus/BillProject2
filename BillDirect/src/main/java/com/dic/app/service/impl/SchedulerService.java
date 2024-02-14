package com.dic.app.service.impl;

import com.dic.app.gis.service.maintaners.impl.TaskController;
import com.dic.app.gis.service.maintaners.impl.TaskService;
import com.dic.app.gis.service.soap.impl.SoapConfig;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.io.File;

@Slf4j
@Service
@RequiredArgsConstructor
public class SchedulerService {

    private final TaskController taskController;
    private final ApplicationContext ctx;
    private final SoapConfig soapConfig;
    private final TaskService taskService;

    /**
     * Проверка необходимости выйти из приложения
     */
    @Scheduled(fixedDelay = 5000)
    public void checkTerminate() {
        // проверка файла "stop" на завершение приложения (для обновления)
        File tempFile = new File("stop");
        boolean exists = tempFile.exists();
        if (exists) {
            log.warn("ВНИМАНИЕ! ЗАПРОШЕНА ОСТАНОВКА ПРИЛОЖЕНИЯ! - БЫЛ СОЗДАН ФАЙЛ c:\\Progs\\BillDirect\\stop");
            SpringApplication.exit(ctx, () -> 0);
        }
    }

    @Scheduled(cron = "${crone.task.check}") // желательно раз в две минуты, иначе flush cache и warning в log. Остальное - вызывается из сброса кэша CacheMng.evictCacheByEntity
    public void searchGisTasks() {
        File tempFile = new File("stopGis");
        boolean exists = tempFile.exists();
        if (soapConfig.isGisKeysLoaded() && !exists) {
            taskController.searchTask();
        }
    }

    @Scheduled(cron = "${crone.house.exp}")
    public void activateRptHouseExp() {
        taskService.activateChildTasks("SYSTEM_RPT_HOUSE_EXP");
    }

    @Scheduled(cron = "${crone.house.imp}")
    public void activateRptHouseImp() {
        taskService.activateChildTasks("SYSTEM_RPT_HOUSE_IMP");
    }

    @Scheduled(cron = "${crone.deb.exch}")
    public void activateRptDebSubExch() {
        taskService.activateChildTasks("SYSTEM_RPT_DEB_SUB_EXCHANGE");
    }

    @Scheduled(cron = "${crone.met.val}")
    public void activateRptMetVal() {
        taskService.activateChildTasks("SYSTEM_RPT_MET_IMP_VAL");
        taskService.activateChildTasks("SYSTEM_RPT_MET_EXP_VAL");
    }



/*
    @Scheduled(fixedDelay = 3000)
    @Transactional(propagation = Propagation.REQUIRES_NEW, rollbackFor = Exception.class)
    public void checkCache() {
        Task task = em.find(Task.class, 1546600);
        Integer lag= Utl.nvl(task.getLagNextStart(),0);
        log.info("lag={}", lag);
//        lag++;
//        task.setLagNextStart(lag);

        taskMng.alterDtNextStart(task, 10);

*/
/*
        Ko houseKo = new Ko();
        houseKo.setGuid("XXXXXXXXXX");
        em.persist(houseKo);

        Eolink eolink = em.find(Eolink.class,707508);
        log.info("1. eolink.cd = {}", eolink.getCd());

        eolink = em.find(Eolink.class,707509);
        log.info("2. eolink.cd = {}", eolink.getCd());
*//*

    }
*/


}
