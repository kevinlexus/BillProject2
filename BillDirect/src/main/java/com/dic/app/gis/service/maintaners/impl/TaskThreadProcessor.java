package com.dic.app.gis.service.maintaners.impl;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ApplicationContext;

import java.io.File;
import java.util.concurrent.LinkedBlockingQueue;

@Slf4j
public class TaskThreadProcessor implements Runnable {

    private final LinkedBlockingQueue<Integer> queueTask;
    private final ApplicationContext context;

    public TaskThreadProcessor(LinkedBlockingQueue<Integer> queueTask, ApplicationContext context) {
        this.queueTask = queueTask;
        this.context = context;
    }

    @Override
    public void run() {
        TaskProcessor taskProcessor = context.getBean(TaskProcessor.class);
        while (true) {
            try {
                File tempFile = new File("stop_gis");
                boolean exists = tempFile.exists();
                if (exists) {
                    log.warn("ВНИМАНИЕ! ОТКЛЮЧЕН ГИС! - БЫЛ СОЗДАН ФАЙЛ c:\\Progs\\BillDirect\\stop_gis");
                    Thread.sleep(10000);
                    continue;
                }
                Integer taskId = queueTask.take();
                log.trace("Поток принял задачу id={}", taskId);
                taskProcessor.processTask(taskId);
            } catch (InterruptedException e) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ex) {
                    log.error("Ошибка в потоке", e);
                }
            }
        }
    }
}
