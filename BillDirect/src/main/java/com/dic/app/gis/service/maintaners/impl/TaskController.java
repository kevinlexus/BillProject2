package com.dic.app.gis.service.maintaners.impl;

import com.dic.app.gis.service.maintaners.TaskControllers;
import com.dic.bill.RequestConfig;
import com.dic.bill.dao.TaskDAO;
import com.dic.app.gis.service.maintaners.TaskMng;
import com.dic.bill.model.exs.Task;
import com.ric.cmn.Utl;
import com.ric.cmn.excp.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.xml.datatype.DatatypeConfigurationException;
import java.util.List;
import java.util.stream.Collectors;

//import com.ric.bill.Config;


/**
 * Основной контроллер заданий
 *
 * @author lev
 * @version 1.12
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class TaskController implements TaskControllers {

    private final TaskDAO taskDao;
    private final TaskControllerProcessor taskControllerProcessor;
    private final TaskMng taskMng;


    // конфиг запроса, сделал здесь, чтобы другие сервисы могли использовать один и тот же запрос
    private RequestConfig reqConfig;

    /**
     * Поиск новых действий для обработки
     *
     * @throws WrongGetMethod
     * @throws CantSendSoap
     * @throws CantPrepSoap
     * @throws WrongParam
     * @ver 1.01
     */
    @Override
    @Transactional
    public void searchTask() {
        log.info("******* Поиск заданий ГИС:");
        // перебрать все необработанные задания
        List<Task> unprocessedTasks = taskDao.getAllUnprocessed()
                .stream().limit(10).collect(Collectors.toList());
        if (log.isTraceEnabled()) {
            log.trace("Необработанные задания");
            unprocessedTasks.forEach(t -> log.trace("id={}, cd={}, atCd={}, dtCr={}, dtUpd={}",
                    t.getId(), t.getCd(), t.getAct().getCd(), t.getDtCrt(), t.getDtUpd()));
            log.trace("Необработанные задания");
        }
        for (Task task : unprocessedTasks) {
            try {
                taskControllerProcessor.processTask(task);
            } catch (ErrorProcessAnswer | DatatypeConfigurationException | CantPrepSoap e) {
                e.printStackTrace();
                log.error("Ошибка при отправке задания Task.id={}, message={}", task.getId(),
                        e.getMessage());
                taskMng.setState(task, "ERR");
                taskMng.setResult(task, e.getMessage());
            } catch (Exception e) {
                log.error("Ошибка выполнения задания Task.id={}, message={}", task.getId(),
                        Utl.getStackTraceString(e));
                String errMess = StringUtils.substring(Utl.getStackTraceString(e), 0, 1000);
                if (!task.getAct().getCd().equals("GIS_SYSTEM_RPT")) {
                    // не помечать ошибкой системные, повторяемые задания
                    taskMng.setState(task, "ERR");
                }
                taskMng.setResult(task, errMess);
            }
        }
    }

    @Override
    public RequestConfig getReqConfig() {
        return reqConfig;
    }

    public void setReqConfig(RequestConfig reqConfig) {
        this.reqConfig = reqConfig;
    }

}
