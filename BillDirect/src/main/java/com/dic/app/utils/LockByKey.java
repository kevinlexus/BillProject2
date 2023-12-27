package com.dic.app.utils;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

@Slf4j
public class LockByKey {
    private boolean needLog = true;

    private static class LockWrapper {
        private final CountDownLatch lock = new CountDownLatch(1);
        private final boolean needLog;

        private LockWrapper(boolean needLog) {
            this.needLog = needLog;
        }

        public void await(String key) {
            if(log.isDebugEnabled() && needLog) {
                log.debug("wait for unlock key={}", key);
            }
            try {
                this.lock.await();
                if(log.isDebugEnabled() && needLog) {
                    log.debug("unlocked key={}", key);
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static final ConcurrentHashMap<String, LockWrapper> locks = new ConcurrentHashMap<>();

    public void lock(String key) {
        if(log.isDebugEnabled() && needLog) {
            log.debug("lock key={}", key);
        }
        LockWrapper wrp = new LockWrapper(needLog);
        while(true) {
            log();
            LockWrapper found = locks.putIfAbsent(key, wrp);
            if(found == null) {
                log.debug("locked key={}", key);
                break; // не было lock, значит теперь там wrp, lock захвачен
            }
            found.await(key); // иначе ждём unlock
        }
    }

    public void unlock(String key) {
        if(log.isDebugEnabled() && needLog) {
            log.debug("unlock key={}", key);
        }
        log();
        LockWrapper lockWrapper = locks.remove(key);
        if (lockWrapper != null) {
            lockWrapper.lock.countDown();
        } else {
            if(needLog) {
                log.error("When key={}, lockWrapper is NULL", key);
            }
        }
    }

    private static void log() {
        log.debug("check locks:");
        locks.forEach((k,v)-> log.debug("key={}, value={}", k, v));
    }
}