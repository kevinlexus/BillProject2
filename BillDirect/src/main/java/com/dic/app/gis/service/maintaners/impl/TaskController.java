package com.dic.app.gis.service.maintaners.impl;

import com.dic.app.service.ConfigApp;
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
import javax.persistence.EntityManager;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;


/**
 * Основной контроллер заданий
 *
 * @author lev
 * @version 1.12
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class TaskController {

    public static final int COUNT_OF_THREADS = 10;
    private final TaskDAO2 taskDao2;
    private final ApplicationContext context;
    private final LinkedBlockingQueue<Integer> queueTask = new LinkedBlockingQueue<>(COUNT_OF_THREADS);
    private final List<Thread> threads;
    private final ConfigApp configApp;
    private final EntityManager em;

    @PostConstruct
    public void init() {
        if (configApp.isGisWorkOnStart()) {
            log.info("Начало создания потоков обработки Task");
            for (int i = 1; i <= COUNT_OF_THREADS; i++) {
                Thread thread = new Thread(new TaskThreadProcessor(queueTask, context));
                threads.add(thread);
                thread.start();
            }
            log.info("Окончание создания потоков обработки Task");
        }
    }

    @PreDestroy
    public void destroy() {
        // если не закрывать таким образом потоки - приложение будет повисать, при попытке shutdown
        if (configApp.isGisWorkOnStart()) {
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
    }

    /**
     * Поиск новых действий для обработки
     */
    @Transactional(readOnly = true)
    public void searchTask() {
        // перебрать все необработанные задания
        List<Task> unprocessedTasks;
        List<Integer> activeTaskIds;
        if (queueTask.isEmpty()) {
            activeTaskIds = List.of(0);
        } else {
            activeTaskIds = new ArrayList<>(queueTask);
        }
        List<Integer> taskIds = new ArrayList<>(taskDao2.getAllUnprocessedNotActiveTaskIds(activeTaskIds));
        unprocessedTasks = taskIds.stream().map(t -> em.find(Task.class, t))
                .filter(t -> t.getPriority() != null || (t.getDtNextStart() == null || t.getDtNextStart().getTime() <= new Date().getTime())) //следующий старт
                .sorted(Comparator.comparing((Task t) -> Utl.nvl(t.getPriority(), 0)).reversed().thenComparing(Task::getId))
                .collect(Collectors.toList());

        try {
            for (Task unprocessedTask : unprocessedTasks) {
                Integer taskId = unprocessedTask.getId();
                queueTask.put(taskId); // если размер очереди = COUNT_OF_THREADS, то здесь поток будет ожидать удаления элемента, после queueTask.take()
                log.trace("Задача id={}, ушла в очередь", taskId);
            }
        } catch (Exception e) {
            log.error("Ошибка во время постановки задания в очередь", e);
        }
    }
}
