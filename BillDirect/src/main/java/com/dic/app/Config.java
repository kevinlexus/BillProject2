package com.dic.app;

import org.springframework.beans.BeansException;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.cache.concurrent.ConcurrentMapCache;
import org.springframework.cache.support.SimpleCacheManager;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportResource;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.scheduling.annotation.AsyncConfigurer;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import java.util.Arrays;
import java.util.concurrent.Executor;

@Configuration
@ComponentScan({"com.dic.bill", "com.dic.app"}) // это нужно чтобы работали Unit-тесты! (по сути можно закомментить)
@EnableJpaRepositories(basePackages="com.dic.bill.dao")
@EnableTransactionManagement
@EnableCaching
@EnableAsync
@EnableScheduling
@EntityScan(basePackages = {"com.dic.bill"})
@ImportResource("file:.\\config\\spring.xml")
public class Config  implements ApplicationContextAware, AsyncConfigurer {

	private static ApplicationContext ctx = null;

    // новый комментарий-3
	@Override
	public void setApplicationContext(ApplicationContext context) throws BeansException {
		ctx = context;
	}

	// 1-2 - добавлено из ветки today3
	@Bean
	public CacheManager cacheManager() {
		SimpleCacheManager cacheManager = new SimpleCacheManager();
		cacheManager.setCaches(Arrays.asList(
				new ConcurrentMapCache("NaborMng.getCached"),
				new ConcurrentMapCache("KartMng.getKartMainLsk"),
				new ConcurrentMapCache("PriceMng.multiplyPrice"),
				new ConcurrentMapCache("HouseMng.findByGuid"),
				new ConcurrentMapCache("ReferenceMng.getUslOrgRedirect")));
		return cacheManager;
	}

	// 2-3 - добавлено из ветки today3
	public static ApplicationContext getContext(){
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
