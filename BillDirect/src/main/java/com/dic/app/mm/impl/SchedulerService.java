package com.dic.app.mm.impl;

import com.dic.app.gis.service.maintaners.TaskControllers;
import com.dic.bill.model.exs.Eolink;
import com.dic.bill.model.scott.Ko;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.persistence.EntityManager;
import java.io.File;

@Slf4j
@Service
@RequiredArgsConstructor
public class SchedulerService {

    private final TaskControllers taskController;
    private final ApplicationContext ctx;
    private final EntityManager em;

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

    // Остановить выполнение загрузки ГИС (для отладки в другом jar)
    @Scheduled(fixedDelay = 5000)
    public void searchGisTasks() {
        File tempFile = new File("stopGis");
        boolean exists = tempFile.exists();
        if (!exists) {
            taskController.searchTask();
        }
    }



/*
    @Scheduled(fixedDelay = 10000)
    @Transactional
    public void checkCache() {
        Ko houseKo = new Ko();
        houseKo.setGuid("XXXXXXXXXX");
        em.persist(houseKo);

        Eolink eolink = em.find(Eolink.class,707508);
        log.info("1. eolink.cd = {}", eolink.getCd());

        eolink = em.find(Eolink.class,707509);
        log.info("2. eolink.cd = {}", eolink.getCd());
    }
*/


}
