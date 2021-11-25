package com.dic.app;

import com.google.common.cache.CacheBuilder;
import org.springframework.beans.BeansException;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.cache.concurrent.ConcurrentMapCache;
import org.springframework.cache.support.SimpleCacheManager;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.*;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.scheduling.annotation.AsyncConfigurer;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import java.util.Arrays;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

@Configuration
@ComponentScan({"com.dic.bill", "com.dic.app"}) // это нужно чтобы работали Unit-тесты! (по сути можно закомментить)
@EnableJpaRepositories(basePackages = "com.dic.bill.dao")
@EnableTransactionManagement
@EnableCaching
@EnableAsync
@EnableScheduling
@EntityScan(basePackages = {"com.dic.bill"})
@PropertySources({
        @PropertySource("file:.\\config\\application.properties"),
        @PropertySource("file:.\\config\\private.properties")
})
public class Config implements ApplicationContextAware, AsyncConfigurer {

    private static ApplicationContext ctx = null;

    @Override
    public void setApplicationContext(ApplicationContext context) throws BeansException {
        ctx = context;
    }

    @Bean
    public CacheManager cacheManager() {
        // fixme отдельно разбораться, как здесь работает кэш 24.11.21
        SimpleCacheManager cacheManager = new SimpleCacheManager();
        cacheManager.setCaches(Arrays.asList(
                new ConcurrentMapCache("NaborMng.getCached"),
                new ConcurrentMapCache("KartMng.getKartMainLsk"),
                new ConcurrentMapCache("PriceMng.multiplyPrice"),
                new ConcurrentMapCache("HouseMng.findByGuid"),
                new ConcurrentMapCache("ReferenceMng.getUslOrgRedirect"),
                new ConcurrentMapCache("ParDAOImpl.getByKlskCd",CacheBuilder
                        .newBuilder().expireAfterWrite(60, TimeUnit.SECONDS).build().asMap(), false),
                new ConcurrentMapCache("MeterLogMngImpl.getKart",CacheBuilder
                        .newBuilder().expireAfterWrite(60, TimeUnit.SECONDS).build().asMap(), false),
                new ConcurrentMapCache("ParMngImpl.isExByCd",CacheBuilder
                        .newBuilder().expireAfterWrite(60, TimeUnit.SECONDS).build().asMap(), false),
                new ConcurrentMapCache("LstMngImpl.getByCD",CacheBuilder
                        .newBuilder().expireAfterWrite(60, TimeUnit.SECONDS).build().asMap(), false),
                new ConcurrentMapCache("EolinkDAOImpl.getEolinkByGuid"),
                new ConcurrentMapCache("UlistMngImpl.getUslByResource"),
                new ConcurrentMapCache("UlistMngImpl.getServCdByResource"),
                new ConcurrentMapCache("UlistMngImpl.getResourceByUsl"),
                new ConcurrentMapCache("TaskDAOImpl.getByKlskCd")
				));
        return cacheManager;
    }

    public static ApplicationContext getContext() {
        return ctx;
    }

    @Bean
    public Executor taskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(10); // минимальное кол-во потоков
        executor.setMaxPoolSize(20); // максимальное кол-во потоков
        executor.setQueueCapacity(50); //
        executor.setThreadNamePrefix("BillDirect-");
        executor.setRejectedExecutionHandler(new RejectedExecutionHandlerImpl());
        executor.initialize();
        return executor;
    }

}
