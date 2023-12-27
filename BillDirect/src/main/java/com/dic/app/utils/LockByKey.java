package com.dic.app.utils;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

@Slf4j
public class LockByKey {

    private static class LockWrapper {
        private final CountDownLatch lock = new CountDownLatch(1);

        public void await(String key) {
            log.debug("wait for unlock key={}", key);
            try {
                this.lock.await();
                log.debug("unlocked key={}", key);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static final ConcurrentHashMap<String, LockWrapper> locks = new ConcurrentHashMap<>();

    public void lock(String key) {
        log.debug("lock key={}", key);
        LockWrapper wrp = new LockWrapper();
        while (true) {
            log();
            LockWrapper found = locks.putIfAbsent(key, wrp);
            if (found == null) {
                log.debug("locked key={}", key);
                break; // не было lock, значит теперь там wrp, lock захвачен
            }
            found.await(key); // иначе ждём unlock
        }
    }

    public void unlock(String key) {
        log.debug("unlock key={}", key);
        log();
        LockWrapper lockWrapper = locks.remove(key);
        if (lockWrapper != null) {
            lockWrapper.lock.countDown();
        } else {
            log.error("When key={}, lockWrapper is NULL", key);
        }
    }

    private static void log() {
        log.debug("check locks:");
        locks.forEach((k, v) -> log.debug("key={}, value={}", k, v));
    }
}