package com.dic.app;

import com.dic.app.gis.service.st.impl.TaskController;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;


@SpringBootApplication
public class BillDirectApplication {

    public static void main(String[] args) {
        ConfigurableApplicationContext app = SpringApplication.run(BillDirectApplication.class, args);

        TaskController taskContr = app.getBean(TaskController.class);
        try {
            taskContr.searchTask();
        } catch (Exception e) {
            e.printStackTrace();
        }


    }

}
