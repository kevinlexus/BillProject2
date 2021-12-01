package com.dic.app.gis.service.maintaners.impl;

import com.dic.app.gis.service.maintaners.TaskControllers;
import com.dic.bill.dao.TaskDAO2;
import com.dic.bill.model.exs.Task;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
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

    public static final int COUNT_OF_THREADS = 5;
    public static final int LIMIT_OF_TASKS = 10;
    private final TaskDAO2 taskDao2;
    private final ApplicationContext context;
    private final LinkedBlockingQueue<Integer> queueTask = new LinkedBlockingQueue<>();


    @PostConstruct
    public void init() {
        log.info("Начало создания потоков обработки Task");
        for (int i = 1; i <= COUNT_OF_THREADS; i++) {
            Thread thread = new Thread(new TaskThreadProcessor(i, queueTask, context));
            thread.start();
        }
        log.info("Окончание создания потоков обработки Task");
    }

    /**
     * Поиск новых действий для обработки
     */
    @Override
    @Transactional
    public void searchTask() {
        if (queueTask.size() < COUNT_OF_THREADS) {
            // перебрать все необработанные задания
            List<Task> unprocessedTasks;
            if (queueTask.size() > 0) {
                unprocessedTasks = taskDao2.getAllUnprocessedAndNotActive(new ArrayList<>(queueTask))
                        .stream().limit(LIMIT_OF_TASKS).collect(Collectors.toList());
            } else {
                unprocessedTasks = taskDao2.getAllUnprocessed()
                        .stream().limit(LIMIT_OF_TASKS).collect(Collectors.toList());
            }

            try {
                for (Task unprocessedTask : unprocessedTasks) {
                    if (!queueTask.contains(unprocessedTask.getId())) {
                        queueTask.put(unprocessedTask.getId());
                        //log.info("Задача id={}, ушла в очередь", unprocessedTask.getId());
                    }
                }
            } catch (Exception e) {
                log.error("Ошибка во время постановки задания в очередь", e);
            }
        }
    }
}
