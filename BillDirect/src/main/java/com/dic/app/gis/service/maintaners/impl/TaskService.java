package com.dic.app.gis.service.maintaners.impl;


import com.dic.app.gis.service.soapbuilders.impl.*;
import com.dic.bill.dao.EolinkDAO;
import com.dic.bill.dao.EolinkDAO2;
import com.dic.bill.dao.TaskDAO2;
import com.dic.bill.dto.HouseUkTaskRec;
import com.dic.bill.model.exs.Eolink;
import com.dic.bill.model.exs.Task;
import com.ric.cmn.Utl;
import com.ric.cmn.excp.WrongParam;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Сервисы обслуживания задач ГИС ЖКХ, (task)
 *
 * @author lev
 * @version 1.3
 * 26.01.2023
 */
@Slf4j
@Service
public class TaskService {

    private HouseManagementAsyncBindingBuilder houseTaskBuilder;
    private final EolinkDAO eolinkDao;
    private final EolinkDAO2 eolinkDao2;
    private final HcsOrgRegistryAsyncBindingBuilder os;
    private final NsiServiceAsyncBindingBuilder nsiSv;
    private final HcsBillsAsyncBuilder billBuilder;
    private final DeviceMeteringAsyncBindingBuilder deviceBuilder;
    private final TaskDAO2 taskDAO2;

    public TaskService(HouseManagementAsyncBindingBuilder houseTaskBuilder, EolinkDAO eolinkDao,
                       EolinkDAO2 eolinkDao2, HcsOrgRegistryAsyncBindingBuilder os, NsiServiceAsyncBindingBuilder nsiSv, HcsBillsAsyncBuilder billBuilder, DeviceMeteringAsyncBindingBuilder deviceBuilder, TaskDAO2 taskDAO2) {
        this.houseTaskBuilder = houseTaskBuilder;
        this.eolinkDao = eolinkDao;
        this.eolinkDao2 = eolinkDao2;
        this.os = os;
        this.nsiSv = nsiSv;
        this.billBuilder = billBuilder;
        this.deviceBuilder = deviceBuilder;
        this.taskDAO2 = taskDAO2;
    }

    /**
     * Проверить наличие заданий
     * и если их нет, - создать
     */

    @Scheduled(cron = "${crone.periodic.check}")
    public void checkPeriodicTasks() throws WrongParam {
        log.info("НАЧАЛО ПРОВЕРКИ ПЕРИОДИЧЕСКИХ ЗАДАНИЙ");
        // удалить задания, которые необходимо пересоздать
        //eolinkDao2.deleteTaskHouseWithMismatchUpdateDate(); // todo убрать удаление, надо что то другое

        // создать по всем домам задания на экспорт объектов дома, счетчиков todo Переделать! По Частному сектору не нужно создавать такие задания!
        createSetOfTasks("SYSTEM_RPT_HOUSE_EXP");

        // создать независимые задания по всем домам, по импорту и экспорту показаний счетчиков
        createParentTasks("GIS_IMP_METER_VALS", "SYSTEM_RPT_MET_IMP_VAL");
        createParentTasks("GIS_EXP_METER_VALS", "SYSTEM_RPT_MET_EXP_VAL");

        // создать зависимые задания по домам МКД, по экспорту лиц.счетов, с указанием Ук - владельца счета
        houseTaskBuilder.createTasks("GIS_EXP_HOUSE", "GIS_EXP_ACCS", "SYSTEM_RPT_HOUSE_EXP");

        // создать независимые задания, по частному сектору, по экспорту лиц.счетов, с указанием Ук - владельца счета
        houseTaskBuilder.createTasks("GIS_EXP_ACCS", true, "SYSTEM_RPT_HOUSE_EXP");

        // создать независимые задания по домам МКД, по импорту лиц.счетов, с указанием Ук - владельца счета
        houseTaskBuilder.createTasks("GIS_IMP_ACCS", false, "SYSTEM_RPT_HOUSE_IMP");

        // создать независимые задания по импорту ответов на запросы о задолженности от УСЗН
        houseTaskBuilder.createTasks("GIS_IMP_DEB_SUB_RESPONSE", false, "SYSTEM_RPT_DEB_SUB_EXCHANGE");

        // создать зависимые задания по экспорту запросов о задолженности от УСЗН
        houseTaskBuilder.createTasks("GIS_IMP_DEB_SUB_RESPONSE", "GIS_EXP_DEB_SUB_REQUEST", "SYSTEM_RPT_DEB_SUB_EXCHANGE");

        // Проверка наличия заданий по импорту ПД
        checkPeriodicImpExpPd();

        // Проверка наличия заданий по экспорту параметров организаций
        os.checkPeriodicTask();
        // Проверка наличия заданий по экспорту справочников организации
        nsiSv.checkPeriodicTask();

        log.info("ОКОНЧАНИЕ ПРОВЕРКИ ПЕРИОДИЧЕСКИХ ЗАДАНИЙ");
    }

    @Transactional
    @Scheduled(cron = "${crone.house.exp}")
    public void activateRptHouseExp() {
        activateChildTasks("SYSTEM_RPT_HOUSE_EXP");
    }

    @Transactional
    @Scheduled(cron = "${crone.house.imp}")
    public void activateRptHouseImp() {
        activateChildTasks("SYSTEM_RPT_HOUSE_IMP");
    }

    @Transactional
    @Scheduled(cron = "${crone.deb.exch}")
    public void activateRptDebSubExch() {
        activateChildTasks("SYSTEM_RPT_DEB_SUB_EXCHANGE");
    }

    @Transactional
    @Scheduled(cron = "${crone.met.val}")
    public void activateRptMetVal() {
        activateChildTasks("SYSTEM_RPT_MET_IMP_VAL");
        activateChildTasks("SYSTEM_RPT_MET_EXP_VAL");
    }

    // создавать по одной, иначе - блокировка Task (нужен коммит)
    private void createSetOfTasks(String rptTaskCd) throws WrongParam {
        for (Eolink eolHouse : eolinkDao.getEolinkByTpWoTaskTp("Дом", "GIS_EXP_HOUSE", rptTaskCd)) {
            houseTaskBuilder.createSetOfTasks(eolHouse, rptTaskCd);
        }
    }

    // создавать по одной, иначе - блокировка Task (нужен коммит)
    private void createParentTasks(String taskCd, String rptTaskCd) {
        for (Eolink eolHouse : eolinkDao.getEolinkByTpWoTaskTp("Дом", taskCd, rptTaskCd)) {
            houseTaskBuilder.createParentTask(eolHouse, rptTaskCd, taskCd);
        }
    }


    /**
     * Проверить наличие заданий на импорт и экспорт ПД
     * и если их нет, - создать
     */
    private void checkPeriodicImpExpPd() {
        // создать по всем домам задания на импорт ПД, если их нет
        createPayDocTasks("GIS_IMP_PAY_DOCS", "SYSTEM_RPT_IMP_PD",
                "импорт ПД");
        // создать по всем домам задания на экспорт ПД, если их нет
        createPayDocTasks("GIS_EXP_PAY_DOCS", "SYSTEM_RPT_EXP_PD",
                "экспорт ПД");
        // создать по всем УК задания на экспорт Извещений по ПД, если их нет, по дням выгрузки
/*
        createTask("GIS_EXP_NOTIF_1", "SYSTEM_RPT_EXP_NOTIF", "STP", "Организация",
                "экспорт Извещений");
        createTask("GIS_EXP_NOTIF_8", "SYSTEM_RPT_EXP_NOTIF", "STP", "Организация",
                "экспорт Извещений");
        createTask("GIS_EXP_NOTIF_16", "SYSTEM_RPT_EXP_NOTIF", "STP", "Организация",
                "экспорт Извещений");
        createTask("GIS_EXP_NOTIF_24", "SYSTEM_RPT_EXP_NOTIF", "STP", "Организация",
                "экспорт Извещений");
*/
    }

    private void createPayDocTasks(String actTp, String parentCD, String purpose) {
        // создать по всем домам задания, если их нет
        // получить дома без заданий
        List<HouseUkTaskRec> lst = eolinkDao2.getHouseByTpWoTaskTp(actTp, 0);
        lst.addAll(eolinkDao2.getHouseByTpWoTaskTp(actTp, 1));
        for (HouseUkTaskRec t : lst) {
            billBuilder.createPayDocSingleTask(actTp, parentCD, "STP", purpose, t);
        }
    }


    /**
     * Активация дочерних и зависимых заданий
     */
    @Transactional(propagation = Propagation.REQUIRED)
    public void activateChildTasks(String taskCd) {
        Optional<Task> foundTaskOpt = taskDAO2.findByCd(taskCd);
        //List<Integer> taskIds = new ArrayList<>();
        if (foundTaskOpt.isPresent()) {
            Task foundTask = foundTaskOpt.get();
            log.info("*** Task.id={}, Повторяемое задание CD={}", foundTask.getId(), foundTask.getAct().getCd());
            /* найти все связи с зависимыми записями, в заданиях которых нет родителя (главные),
               а так же если у этих заданий либо не имеется зависимых заданий, либо имеются и
               они НЕ находятся в статусах INS, ACK (т.е. на обработке)
               (по определённому типу связи)
            */
            foundTask.getInside().stream()
                    .filter(t -> t.getTp().getCd().equals("Связь повторяемого задания"))
                    .filter(t -> t.getChild().getParent() == null) // только главные
                    .filter(t -> t.getChild().getMaster() == null) // только независимые (где не заполнен DEP_ID)
                    .forEach(t -> {
                        log.trace("*** t.getChild().getId()={}", t.getChild().getId());
                        ArrayList<Task> taskLst = new ArrayList<>(10);
                        if (activateTask(t.getChild(), taskLst)) {
                            // разрешить запуск по всем дочерним заданиям
                            taskLst.forEach(t2 -> {
                                // не понятно, почему, не проставлялся статус "INS", пришлось получить Task с помощью getOne
                                // и проставить статус ред. 26.01.23
                                Task task2 = taskDAO2.getOne(t2.getId());
                                task2.setState("INS");
                                //taskIds.add(t2.getId());
                                log.trace("*** Разрешено!!!!!!!: id={}", t2.getId());
                                log.trace("*** state task.id={}, task.state={}", task2.getId(), task2.getState());
                            });
                        }
                    });
        } else {
            log.error("Не найдено повторяемое задание с CD={}", taskCd);
        }
    }

    /**
     * Рекурсивная активация заданий
     *
     * @param task    задание
     * @param taskLst
     * @return разрешить активацию
     */
    private boolean activateTask(Task task, List<Task> taskLst) {
        log.trace("*** activateTask: task.id={}, state={}", task.getId(), task.getState());
        if (Utl.in(task.getState(), "ACK")) {
            // текущее уже выполняется
            log.trace("Ожидается завершение выполнения задания (дочерних заданий): id={}", task.getId());
            return false;
        } else {
            // дочерние задания
            for (Task child : task.getChild()) {
                if (!activateTask(child, taskLst)) {
                    // дочернее уже выполняется
                    log.trace("*** Дочернее уже выполняется: id={}", child.getId());
                    return false;
                }
            }
            // зависимые по DEP_ID задания
            for (Task dep : task.getSlave()) {
                if (!activateTask(dep, taskLst)) {
                    // дочернее по DEP_ID уже выполняется
                    log.trace("*** По DEP_ID уже выполняется: id={}", dep.getId());
                    return false;
                }
            }
            taskLst.add(task);
            log.trace("*** Разрешено выполнение: id={}", task.getId());
            return true;
        }
    }
}