package com.dic.app.service.impl;

import com.dic.app.gis.service.maintaners.impl.TaskController;
import com.dic.app.service.ConfigApp;
import com.dic.bill.model.exs.Task;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hibernate.SessionFactory;
import org.springframework.stereotype.Service;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
public class CacheMng {

    @PersistenceContext
    private EntityManager em;

    private final ConfigApp configApp;
    private final TaskController taskController;

    public String evictCacheByEntity(String entityClassName, String id) {
        Map<String, Object> mapClassId = configApp.getMapClassId();

        Object fieldType = mapClassId.get(entityClassName);
        if (fieldType == null) {
            throw new RuntimeException("Несуществующий класс в mapClassId=" + entityClassName);
        }

        Object objectId;
        if (fieldType.equals(String.class)) {
            objectId = id;
        } else if (fieldType.equals(Integer.class)) {
            objectId = Integer.valueOf(id);
        } else if (fieldType.equals(Long.class)) {
            objectId = Long.valueOf(id);
        } else {
            throw new RuntimeException("Недопустимый класс обработки Id=" + fieldType.getClass().getName());
        }
        Class<?> clasz;
        try {
            clasz = Class.forName(entityClassName);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Не найден класс в classloader, по имени=" + entityClassName);
        }

        em.getEntityManagerFactory().getCache().evict(clasz, objectId);
        if (em.getEntityManagerFactory().getCache().contains(clasz, objectId)) {
            log.warn("****** ВНИМАНИЕ! Hibernate L2 Кэш по {}, id={} НЕ БЫЛ очищен! ******", entityClassName, id);
        } else {
            log.info("Hibernate L2 Кэш по {}, id={} очищен!", entityClassName, id);
        }
        if (clasz.equals(Task.class)) {
            // здесь не проверяется статус "INS", позже, в TaskProcessor
            taskController.putTaskToWork(Integer.valueOf(id));
        }

        return "OK";
    }

    public String evictL2C() {
        SessionFactory sessionFactory = em.getEntityManagerFactory().unwrap(SessionFactory.class);
        sessionFactory.getCache().evictRegion("BillDirectEntitiesCache");
        log.info("Hibernate L2 Кэш очищен!");
        return "OK";
    }

    public String evictRegion(String regionName) {
        SessionFactory sessionFactory = em.getEntityManagerFactory().unwrap(SessionFactory.class);
        sessionFactory.getCache().evictRegion(regionName);
        log.info("Hibernate L2 Кэш очищен по региону {}", regionName);
        return "OK";
    }
}
