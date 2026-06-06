package week0;

import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

class MonoFromFutureTest {

    // lazy: () -> future 형태 — subscribe 시점에 CompletableFuture가 생성된다
    @Test
    void lazyFromFutureExecutesOnSubscribe() {
        AtomicBoolean futureCreated = new AtomicBoolean(false);

        Mono<String> mono = Mono.fromFuture(() -> {
            futureCreated.set(true);
            return CompletableFuture.completedFuture("result");
        });

        assertFalse(futureCreated.get(), "subscribe 전에는 Future가 생성되지 않는다");

        StepVerifier.create(mono)
                .expectNext("result")
                .expectComplete()
                .verify();

        assertTrue(futureCreated.get(), "subscribe 후에 Future가 생성된다");
    }

    // eager: 이미 생성된 Future를 넘기면 subscribe 전에 즉시 실행이 시작된다
    @Test
    void eagerFromFutureExecutesImmediately() {
        AtomicBoolean taskStarted = new AtomicBoolean(false);

        // Future 생성 즉시 supplyAsync가 스레드 풀에서 실행 시작됨
        CompletableFuture<String> future = CompletableFuture.supplyAsync(() -> {
            taskStarted.set(true);
            return "result";
        });

        // subscribe 전에 이미 실행됨을 확인 (join으로 완료 대기 후 단언)
        future.join();
        assertTrue(taskStarted.get(), "subscribe 전에 이미 실행됨");

        Mono<String> mono = Mono.fromFuture(future);

        StepVerifier.create(mono)
                .expectNext("result")
                .expectComplete()
                .verify();
    }

    // 실전 패턴: supplyAsync 결과를 Mono로 받기
    @Test
    void fromFutureIntegratesAsyncResult() {
        Mono<Integer> mono = Mono.fromFuture(() ->
                CompletableFuture.supplyAsync(() -> 21 + 21)
        );

        StepVerifier.create(mono)
                .expectNext(42)
                .expectComplete()
                .verify();
    }
}