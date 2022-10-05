package com.dic.app.gis.service.maintaners.impl;

import com.dic.app.gis.service.maintaners.TaskControllers;
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
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
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
    private final TaskDAO2 taskDao2;
    private final ApplicationContext context;
    private final LinkedBlockingQueue<Integer> queueTask = new LinkedBlockingQueue<>();
    @Getter
    private static final Map<Integer, Integer> taskInWork = new ConcurrentHashMap<>();
    private final List<Thread> threads;
    private final ConfigApp configApp;


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
    @Override
    @Transactional
    public void searchTask() {
        if (queueTask.size() < COUNT_OF_THREADS) {
            // перебрать все необработанные задания
            List<Task> unprocessedTasks;
            //log.info("queueTask.size()={}", queueTask.size());
            List<Task> predFilter;
            if (queueTask.size() > 0) {
                predFilter = taskDao2.getAllUnprocessedAndNotActive(new ArrayList<>(queueTask)).stream().collect(Collectors.toList());
                //predFilter.forEach(t -> log.info("1 predFilter taskId={}", t.getId()));
                unprocessedTasks = predFilter
                        .stream()
                        .filter(t -> t.getPriority() != null || (t.getDtNextStart() == null || t.getDtNextStart().getTime() <= new Date().getTime())) //следующий старт
                        .sorted(Comparator.comparing((Task t) -> Utl.nvl(t.getPriority(), 0)).reversed().thenComparing(Task::getId))
                        .collect(Collectors.toList());
                unprocessedTasks.forEach(t -> log.info("2 afterFilter taskId={}", t.getId()));
            } else {
                predFilter = taskDao2.getAllUnprocessed().stream().collect(Collectors.toList());
                //predFilter.forEach(t -> log.info("3 predFilter taskId={}", t.getId()));
                unprocessedTasks = predFilter
                        .stream()
                        .filter(t -> t.getPriority() != null || (t.getDtNextStart() == null || t.getDtNextStart().getTime() <= new Date().getTime())) //следующий старт
                        .sorted(Comparator.comparing((Task t) -> Utl.nvl(t.getPriority(), 0)).reversed().thenComparing(Task::getId))
                        .collect(Collectors.toList());
                //unprocessedTasks.forEach(t -> log.info("4 afterFilter taskId={}", t.getId()));
            }

            try {
                for (Task unprocessedTask : unprocessedTasks) {
                    Integer taskId = unprocessedTask.getId();
                    putTaskToWork(taskId);
                }
            } catch (Exception e) {
                log.error("Ошибка во время постановки задания в очередь", e);
            }
        }
    }

    private int putTaskToWork(Integer taskId) {
        AtomicInteger count = new AtomicInteger(0);
        taskInWork.computeIfAbsent(taskId, t -> {
            Optional<Task> task = taskDao2.findById(taskId);
            task.ifPresent(d -> {
                try {
                    queueTask.put(taskId);
                    count.incrementAndGet();
                } catch (InterruptedException e) {
                    log.error("Ошибка отправки задачи в очередь", e);
                }
                log.trace("Задача id={}, ушла в очередь", taskId);
            });
            return taskId;
        });
        return count.get();
    }
}
