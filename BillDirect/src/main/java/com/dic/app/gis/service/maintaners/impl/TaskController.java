package com.dic.app.gis.service.maintaners.impl;

import com.dic.app.gis.service.maintaners.TaskControllers;
import com.dic.bill.dao.TaskDAO2;
import com.dic.bill.model.exs.Task;
import com.ric.cmn.Utl;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
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

    public static final int COUNT_OF_THREADS = 10;
    public static final int LIMIT_OF_TASKS = 10;
    private final TaskDAO2 taskDao2;
    private final ApplicationContext context;
    private final LinkedBlockingQueue<Integer> queueTask = new LinkedBlockingQueue<>();
    @Getter
    private static final Map<Integer, Integer> taskInWork = new ConcurrentHashMap<>();

    private final List<Thread> threads;


    @PostConstruct
    public void init() {
        log.info("Начало создания потоков обработки Task");
        for (int i = 1; i <= COUNT_OF_THREADS; i++) {
            Thread thread = new Thread(new TaskThreadProcessor(queueTask, context));
            threads.add(thread);
            thread.start();
        }
        log.info("Окончание создания потоков обработки Task");
    }

    @PreDestroy
    public void destroy() {
        // если не закрывать таким образом потоки - приложение будет повисать, при попытке shutdown
        log.info("Начало закрытия потоков обработки Task");
        for (Thread thread : threads) {
            try {
                thread.stop(); //todo переделать
            } catch (Exception e) {
                log.error("Ошибка во время закрытия потоков обработки Task");
            }
        }
        log.info("Окончание закрытия потоков обработки Task");
    }

    /**
     * Поиск новых действий для обработки
     */
    @Override
    @Transactional
    public void searchTask() {
        //log.info("queueTask.size={}", queueTask.size());
        taskInWork.forEach((key, value) -> log.info("taskInWork taskId={}", key));
        queueTask.forEach(t -> log.info("queueTask taskId={}", t));
        if (queueTask.size() < COUNT_OF_THREADS) {
            // перебрать все необработанные задания
            List<Task> unprocessedTasks;
            if (queueTask.size() > 0) {
                unprocessedTasks = taskDao2.getAllUnprocessedAndNotActive(new ArrayList<>(queueTask))
                        .stream()
                        .filter(t -> t.getPriority() != null || (t.getDtNextStart() == null || t.getDtNextStart().getTime() <= new Date().getTime())) //следующий старт
                        .sorted(Comparator.comparing((Task t) -> Utl.nvl(t.getPriority(), 0)).reversed().thenComparing(Task::getId))
                        .limit(LIMIT_OF_TASKS).collect(Collectors.toList());
            } else {
                unprocessedTasks = taskDao2.getAllUnprocessed()
                        .stream()
                        .filter(t -> t.getPriority() != null || (t.getDtNextStart() == null || t.getDtNextStart().getTime() <= new Date().getTime())) //следующий старт
                        .sorted(Comparator.comparing((Task t) -> Utl.nvl(t.getPriority(), 0)).reversed().thenComparing(Task::getId))
                        .limit(LIMIT_OF_TASKS).collect(Collectors.toList());
            }

            try {
                for (Task unprocessedTask : unprocessedTasks) {
                    Integer taskId = unprocessedTask.getId();
                    if (!queueTask.contains(taskId) && !taskInWork.containsKey(taskId)) {
                        taskInWork.put(taskId, taskId);
                        queueTask.put(taskId);
                        log.info("Задача id={}, ушла в очередь", taskId);
                    }
                }
            } catch (Exception e) {
                log.error("Ошибка во время постановки задания в очередь", e);
            }
        }
    }
}
