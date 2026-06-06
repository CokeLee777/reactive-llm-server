package week0;

import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class BlockingIoTest {

    // blocking 작업은 boundedElastic 스케줄러에서 실행해야 한다
    // Mono.fromCallable(): Callable(checked exception 허용)을 Mono로 감싼다
    @Test
    void blockingWorkRunsOnBoundedElastic() {
        AtomicReference<String> threadName = new AtomicReference<>();

        Mono<String> mono = Mono.fromCallable(() -> {
                    threadName.set(Thread.currentThread().getName());
                    Thread.sleep(10); // blocking 작업 시뮬레이션
                    return "result";
                })
                .subscribeOn(Schedulers.boundedElastic());

        StepVerifier.create(mono)
                .expectNext("result")
                .expectComplete()
                .verify();

        assertTrue(
                threadName.get().startsWith("boundedElastic-"),
                "실제 스레드명: " + threadName.get()
        );
    }

    // subscribeOn 없이 blocking하면 subscribe를 호출한 스레드(테스트 스레드)에서 실행된다
    @Test
    void withoutSchedulerRunsOnCallerThread() {
        AtomicReference<String> threadName = new AtomicReference<>();
        String callerThread = Thread.currentThread().getName();

        Mono<String> mono = Mono.fromCallable(() -> {
            threadName.set(Thread.currentThread().getName());
            return "result";
        });
        // subscribeOn 없음 — 호출 스레드에서 동기 실행

        StepVerifier.create(mono)
                .expectNext("result")
                .expectComplete()
                .verify();

        assertEquals(
                callerThread,
                threadName.get(),
                "subscribeOn 없으면 호출 스레드(" + callerThread + ")에서 실행된다"
        );
    }
}