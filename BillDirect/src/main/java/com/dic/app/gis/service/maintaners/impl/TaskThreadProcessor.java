package com.dic.app.gis.service.maintaners.impl;

import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationContext;

import java.util.concurrent.LinkedBlockingQueue;

@Slf4j
public class TaskThreadProcessor implements Runnable {

    private final LinkedBlockingQueue<Integer> queueTask;
    private final int num;
    private final ApplicationContext context;

    public TaskThreadProcessor(int num, LinkedBlockingQueue<Integer> queueTask, ApplicationContext context) {
        this.queueTask = queueTask;
        this.num = num;
        this.context = context;
    }

    @Override
    public void run() {
        TaskProcessor taskProcessor = context.getBean(TaskProcessor.class);
        while (true) {
            try {
                Integer taskId = queueTask.take();
                taskProcessor.processTask(taskId);
                //log.info("Поток {}, принял задачу id={}", num, taskId);
            } catch (InterruptedException e) {
                //log.info("Поток {}, ОСТАНОВЛЕН", num); // fixme исправить или убрать коммент
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException ex) {
                    log.error("Ошибка в потоке", e);
                }
            }
        }
    }
}
