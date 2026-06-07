package alex.exam;

import alex.exam.schedulers.ComputationScheduler;
import alex.exam.schedulers.IoScheduler;
import alex.exam.schedulers.SingleThreadScheduler;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

class ObservableTest {

    @Test
    void testCreateAndSubscribe() {
        List<Integer> items = new ArrayList<>();

        Observable<Integer> observable = Observable.create(observer -> {
            observer.onNext(1);
            observer.onNext(2);
            observer.onNext(3);
            observer.onComplete();
        });

        observable.subscribe(items::add);

        assertThat(items).containsExactly(1, 2, 3);
    }

    @Test
    void testMap() {
        List<String> items = new ArrayList<>();

        Observable<Integer> source = Observable.create(observer -> {
            observer.onNext(1);
            observer.onNext(2);
            observer.onNext(3);
            observer.onComplete();
        });

        source.map(x -> "Number: " + x)
                .subscribe(items::add);

        assertThat(items).containsExactly("Number: 1", "Number: 2", "Number: 3");
    }

    @Test
    void testFilter() {
        List<Integer> items = new ArrayList<>();

        Observable<Integer> source = Observable.create(observer -> {
            observer.onNext(1);
            observer.onNext(2);
            observer.onNext(3);
            observer.onNext(4);
            observer.onNext(5);
            observer.onComplete();
        });

        source.filter(x -> x % 2 == 0)
                .subscribe(items::add);

        assertThat(items).containsExactly(2, 4);
    }

    @Test
    void testFlatMap() {
        List<Integer> items = new ArrayList<>();

        Observable<Integer> source = Observable.create(observer -> {
            observer.onNext(1);
            observer.onNext(2);
            observer.onComplete();
        });

        source.flatMap(x -> Observable.create(inner -> {
            inner.onNext(x * 10);
            inner.onNext(x * 20);
            inner.onComplete();
        })).subscribe(items::add);

        assertThat(items).containsExactly(10, 20, 20, 40);
    }

    @Test
    void testErrorHandling() {
        AtomicReference<Throwable> errorRef = new AtomicReference<>();

        Observable<Integer> source = Observable.create(observer -> {
            observer.onNext(1);
            observer.onError(new RuntimeException("Test error"));
        });

        source.subscribe(item -> {}, errorRef::set, () -> {});

        assertThat(errorRef.get()).isInstanceOf(RuntimeException.class);
        assertThat(errorRef.get().getMessage()).isEqualTo("Test error");
    }

    @Test
    void testSubscribeOnIO() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<String> threadName = new AtomicReference<>();

        Observable<Integer> source = Observable.create(observer -> {
            threadName.set(Thread.currentThread().getName());
            observer.onNext(1);
            observer.onComplete();
            latch.countDown();
        });

        source.subscribeOn(new IoScheduler())
                .subscribe(item -> {});

        latch.await(2, TimeUnit.SECONDS);
        assertThat(threadName.get()).contains("pool");
    }

    @Test
    void testObserveOn() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<String> processingThread = new AtomicReference<>();

        Observable<Integer> source = Observable.create(observer -> {
            observer.onNext(1);
            observer.onComplete();
        });

        source.observeOn(new ComputationScheduler())
                .subscribe(item -> {
                    processingThread.set(Thread.currentThread().getName());
                    latch.countDown();
                });

        latch.await(2, TimeUnit.SECONDS);
        assertThat(processingThread.get()).contains("pool");
    }

    @Test
    void testDisposable() {
        List<Integer> items = new ArrayList<>();

        Observable<Integer> source = Observable.create(observer -> {
            for (int i = 1; i <= 10; i++) {
                observer.onNext(i);
            }
            observer.onComplete();
        });

        Disposable disposable = source.subscribe(items::add);

        // Отменяем после 5 элементов
        disposable.dispose();

        assertThat(items.size()).isLessThan(10);
        assertThat(disposable.isDisposed()).isTrue();
    }

    @Test
    void testChainOperators() {
        List<String> items = new ArrayList<>();

        Observable<Integer> source = Observable.create(observer -> {
            for (int i = 1; i <= 10; i++) {
                observer.onNext(i);
            }
            observer.onComplete();
        });

        source.filter(x -> x % 2 == 0)
                .map(x -> x * 10)
                .filter(x -> x > 30)
                .map(x -> "Result: " + x)
                .subscribe(items::add);

        assertThat(items).containsExactly("Result: 40", "Result: 60", "Result: 80", "Result: 100");
    }

    @Test
    void testMultithreadingWithSubscribeOnAndObserveOn() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        List<String> threadNames = new ArrayList<>();

        Observable<Integer> source = Observable.create(observer -> {
            threadNames.add("produce:" + Thread.currentThread().getName());
            observer.onNext(1);
            observer.onNext(2);
            observer.onComplete();
        });

        source.subscribeOn(new IoScheduler())
                .observeOn(new ComputationScheduler())
                .subscribe(item -> {
                    threadNames.add("consume:" + Thread.currentThread().getName());
                    latch.countDown();
                });

        latch.await(2, TimeUnit.SECONDS);

        assertThat(threadNames).hasSize(3);
        assertThat(threadNames.get(0)).startsWith("produce:");
        assertThat(threadNames.get(1)).startsWith("consume:");
    }
}