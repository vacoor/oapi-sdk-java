package com.lark.oapi.ws;

import org.junit.Test;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import static org.junit.Assert.*;

public class TestClientClose {
    private Client client() {
        return new Client.Builder("app_id", "app_secret").autoReconnect(true).build();
    }

    @Test
    public void defaultClosePreservesExecutorAndStopsReconnect() throws Exception {
        Client client = client();
        try {
            client.close();
            assertTrue(client.userClosed);
            assertFalse(client.shouldReconnect());
            assertFalse(client.executor.isShutdown());
            assertEquals("usable", client.executor.submit(() -> "usable").get(2, TimeUnit.SECONDS));
        } finally {
            client.executor.shutdownNow();
        }
    }

    @Test
    public void falseShutdownPreservesExecutor() {
        Client client = client();
        try {
            client.close(false);
            client.close(false, 0);
            assertTrue(client.userClosed);
            assertFalse(client.executor.isShutdown());
        } finally {
            client.executor.shutdownNow();
        }
    }

    @Test
    public void gracefulShutdownLetsExistingTaskFinish() throws Exception {
        Client client = client();
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        Future<Boolean> task = client.executor.submit(() -> {
            started.countDown();
            release.await();
            return true;
        });
        Thread releaser = new Thread(() -> {
            while (!client.executor.isShutdown()) Thread.yield();
            release.countDown();
        });
        releaser.setDaemon(true);
        try {
            assertTrue(started.await(2, TimeUnit.SECONDS));
            releaser.start();
            client.close(true, 2000);
            assertTrue(task.get(2, TimeUnit.SECONDS));
            assertTrue(client.executor.isTerminated());
        } finally {
            release.countDown();
            client.executor.shutdownNow();
            releaser.join(2000);
        }
    }

    @Test
    public void zeroTimeoutInterruptsUnfinishedTask() throws Exception {
        Client client = client();
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch interrupted = new CountDownLatch(1);
        client.executor.execute(() -> {
            started.countDown();
            try {
                new CountDownLatch(1).await();
            } catch (InterruptedException expected) {
                interrupted.countDown();
            }
        });
        try {
            assertTrue(started.await(2, TimeUnit.SECONDS));
            client.close(true, 0);
            assertTrue(interrupted.await(2, TimeUnit.SECONDS));
            assertTrue(client.executor.awaitTermination(2, TimeUnit.SECONDS));
        } finally {
            client.executor.shutdownNow();
        }
    }

    @Test
    public void interruptionForcesShutdownAndRestoresFlag() throws Exception {
        Client client = client();
        CountDownLatch started = new CountDownLatch(1);
        client.executor.execute(() -> {
            started.countDown();
            try { new CountDownLatch(1).await(); }
            catch (InterruptedException expected) { Thread.currentThread().interrupt(); }
        });
        try {
            assertTrue(started.await(2, TimeUnit.SECONDS));
            Thread.currentThread().interrupt();
            client.close(true, 2000);
            assertTrue(Thread.currentThread().isInterrupted());
            assertTrue(client.executor.isShutdown());
        } finally {
            Thread.interrupted();
            client.executor.shutdownNow();
            assertTrue(client.executor.awaitTermination(2, TimeUnit.SECONDS));
        }
    }

    @Test
    public void shutdownIsRepeatableAndPreventsRestart() {
        Client client = client();
        try {
            client.close(true);
            client.close(true);
            assertTrue(client.executor.isTerminated());
            try {
                client.start();
                fail("Restart after executor shutdown must fail before network access");
            } catch (IllegalStateException expected) {
                assertTrue(client.userClosed);
            }
        } finally {
            client.executor.shutdownNow();
        }
    }

    @Test
    public void negativeTimeoutDoesNotCloseClient() {
        Client client = client();
        try {
            try {
                client.close(true, -1);
                fail("Negative timeout must be rejected");
            } catch (IllegalArgumentException expected) {
                assertFalse(client.userClosed);
                assertFalse(client.executor.isShutdown());
            }
        } finally {
            client.executor.shutdownNow();
        }
    }
}
