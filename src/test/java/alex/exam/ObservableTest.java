package alex.exam;

import alex.exam.schedulers.ComputationScheduler;
import alex.exam.schedulers.IoScheduler;
import alex.exam.schedulers.SingleThreadScheduler;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

class ObservableTest {

    @Test
    void testCreateAndSubscribe() {
        List<Integer> items = new ArrayList<>();

        // Явно указываем тип для create
        Observable<Integer> observable = Observable.<Integer>create((observer, disposable) -> {
            observer.onNext(1);
            observer.onNext(2);
            observer.onNext(3);
            observer.onComplete();
        });

        observable.subscribe((Integer item) -> items.add(item));

        assertThat(items).containsExactly(1, 2, 3);
    }

    @Test
    void testMap() {
        List<String> items = new ArrayList<>();

        Observable<Integer> source = Observable.<Integer>create((observer, disposable) -> {
            observer.onNext(1);
            observer.onNext(2);
            observer.onNext(3);
            observer.onComplete();
        });

        source.<String>map(x -> "Number: " + x)
                .subscribe((String item) -> items.add(item));

        assertThat(items).containsExactly("Number: 1", "Number: 2", "Number: 3");
    }

    @Test
    void testFilter() {
        List<Integer> items = new ArrayList<>();

        Observable<Integer> source = Observable.<Integer>create((observer, disposable) -> {
            observer.onNext(1);
            observer.onNext(2);
            observer.onNext(3);
            observer.onNext(4);
            observer.onNext(5);
            observer.onComplete();
        });

        source.filter(x -> x % 2 == 0)
                .subscribe((Integer item) -> items.add(item));

        assertThat(items).containsExactly(2, 4);
    }

    @Test
    void testFlatMap() {
        List<Integer> items = new ArrayList<>();

        Observable<Integer> source = Observable.<Integer>create((observer, disposable) -> {
            observer.onNext(1);
            observer.onNext(2);
            observer.onComplete();
        });

        source.flatMap((Integer x) -> {
            return Observable.<Integer>create((inner, d) -> {
                inner.onNext(x * 10);
                inner.onNext(x * 20);
                inner.onComplete();
            });
        }).subscribe((Integer item) -> items.add(item));

        assertThat(items).containsExactly(10, 20, 20, 40);
    }

    @Test
    void testFlatMapWithStrings() {
        List<String> items = new ArrayList<>();

        Observable<String> source = Observable.<String>create((observer, disposable) -> {
            observer.onNext("Hello");
            observer.onNext("World");
            observer.onComplete();
        });

        source.flatMap((String word) -> {
            return Observable.<String>create((inner, d) -> {
                for (char c : word.toCharArray()) {
                    inner.onNext(String.valueOf(c));
                }
                inner.onComplete();
            });
        }).subscribe((String item) -> items.add(item));

        assertThat(items).containsExactly("H", "e", "l", "l", "o", "W", "o", "r", "l", "d");
    }

    @Test
    void testErrorHandling() {
        AtomicReference<Throwable> errorRef = new AtomicReference<>();

        Observable<Integer> source = Observable.<Integer>create((observer, disposable) -> {
            observer.onNext(1);
            observer.onError(new RuntimeException("Test error"));
        });

        source.subscribe(
                (Integer item) -> {},
                (Throwable error) -> errorRef.set(error),
                () -> {}
        );

        assertThat(errorRef.get()).isInstanceOf(RuntimeException.class);
        assertThat(errorRef.get().getMessage()).isEqualTo("Test error");
    }

    @Test
    void testSubscribeOnIO() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<String> threadName = new AtomicReference<>();

        Observable<Integer> source = Observable.<Integer>create((observer, disposable) -> {
            threadName.set(Thread.currentThread().getName());
            observer.onNext(1);
            observer.onComplete();
            latch.countDown();
        });

        source.subscribeOn(new IoScheduler())
                .subscribe((Integer item) -> {});

        latch.await(2, TimeUnit.SECONDS);
        assertThat(threadName.get()).contains("pool");
    }

    @Test
    void testObserveOn() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<String> processingThread = new AtomicReference<>();

        Observable<Integer> source = Observable.<Integer>create((observer, disposable) -> {
            observer.onNext(1);
            observer.onComplete();
        });

        source.observeOn(new ComputationScheduler())
                .subscribe((Integer item) -> {
                    processingThread.set(Thread.currentThread().getName());
                    latch.countDown();
                });

        latch.await(2, TimeUnit.SECONDS);
        assertThat(processingThread.get()).contains("pool");
    }

    @Test
    void testDisposable() {
        List<Integer> items = new ArrayList<>();
        AtomicBoolean stopped = new AtomicBoolean(false);

        Observable<Integer> source = Observable.<Integer>create((observer, disposable) -> {
            for (int i = 1; i <= 10; i++) {
                if (stopped.get()) {
                    break;
                }
                observer.onNext(i);
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    break;
                }
            }
            if (!stopped.get()) {
                observer.onComplete();
            }
        });

        Disposable disposable = source.subscribe((Integer item) -> {
            items.add(item);
            if (item == 3) {
                stopped.set(true);
            }
        });

        try {
            Thread.sleep(200);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // Проверяем, что после item=3 больше не пришло элементов
        assertThat(items).containsExactly(1, 2, 3);
        // Проверяем, что stopped стал true (отмена сработала)
        assertThat(stopped.get()).isTrue();
    }

    @Test
    void testChainOperators() {
        List<String> items = new ArrayList<>();

        Observable<Integer> source = Observable.<Integer>create((observer, disposable) -> {
            for (int i = 1; i <= 10; i++) {
                observer.onNext(i);
            }
            observer.onComplete();
        });

        source.filter(x -> x % 2 == 0)
                .<Integer>map(x -> x * 10)
                .filter(x -> x > 30)
                .<String>map(x -> "Result: " + x)
                .subscribe((String item) -> items.add(item));

        assertThat(items).containsExactly("Result: 40", "Result: 60", "Result: 80", "Result: 100");
    }

    @Test
    void testSubscribeOnAndObserveOn() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(2);
        List<String> threadNames = new ArrayList<>();

        Observable<Integer> source = Observable.<Integer>create((observer, disposable) -> {
            threadNames.add("produce:" + Thread.currentThread().getName());
            observer.onNext(1);
            observer.onNext(2);
            observer.onComplete();
        });

        source.subscribeOn(new IoScheduler())
                .observeOn(new ComputationScheduler())
                .subscribe((Integer item) -> {
                    threadNames.add("consume:" + Thread.currentThread().getName());
                    latch.countDown();
                });

        latch.await(2, TimeUnit.SECONDS);

        assertThat(threadNames).hasSize(3);
        assertThat(threadNames.get(0)).startsWith("produce:");
        assertThat(threadNames.get(1)).startsWith("consume:");
    }

    @Test
    void testSingleThreadScheduler() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(3);
        List<String> threadNames = new ArrayList<>();

        Observable<Integer> source = Observable.<Integer>create((observer, disposable) -> {
            threadNames.add("emit:" + Thread.currentThread().getName());
            observer.onNext(1);
            observer.onNext(2);
            observer.onNext(3);
            observer.onComplete();
        });

        source.subscribeOn(new SingleThreadScheduler())
                .subscribe((Integer item) -> {
                    threadNames.add("process:" + Thread.currentThread().getName());
                    latch.countDown();
                });

        latch.await(2, TimeUnit.SECONDS);

        String firstProcessThread = null;
        for (String name : threadNames) {
            if (name.startsWith("process:")) {
                if (firstProcessThread == null) {
                    firstProcessThread = name;
                } else {
                    assertThat(name).isEqualTo(firstProcessThread);
                }
            }
        }
    }

    @Test
    void testMapWithError() {
        List<String> items = new ArrayList<>();
        AtomicReference<Throwable> errorRef = new AtomicReference<>();

        Observable<Integer> source = Observable.<Integer>create((observer, disposable) -> {
            observer.onNext(1);
            observer.onNext(2);
            observer.onNext(3);
            observer.onComplete();
        });

        source.<String>map(x -> {
            if (x == 2) {
                throw new RuntimeException("Map error at " + x);
            }
            return "Value: " + x;
        }).subscribe(
                (String item) -> items.add(item),
                (Throwable error) -> errorRef.set(error),
                () -> {}
        );

        // Должен прийти только первый элемент, так как на втором произошла ошибка
        assertThat(items).containsExactly("Value: 1");
        assertThat(errorRef.get()).isNotNull();
        assertThat(errorRef.get().getMessage()).isEqualTo("Map error at 2");
    }

    @Test
    void testFilterWithError() {
        List<Integer> items = new ArrayList<>();
        AtomicReference<Throwable> errorRef = new AtomicReference<>();

        Observable<Integer> source = Observable.<Integer>create((observer, disposable) -> {
            observer.onNext(1);
            observer.onNext(2);
            observer.onNext(3);
            observer.onComplete();
        });

        source.filter(x -> {
            if (x == 2) {
                throw new RuntimeException("Filter error at " + x);
            }
            return x > 1;
        }).subscribe(
                (Integer item) -> items.add(item),
                (Throwable error) -> errorRef.set(error),
                () -> {}
        );

        // После ошибки поток должен остановиться, элементы после ошибки не приходят
        assertThat(items).isEmpty();
        assertThat(errorRef.get()).isNotNull();
        assertThat(errorRef.get().getMessage()).isEqualTo("Filter error at 2");
    }

    @Test
    void testMultipleSubscribers() {
        List<Integer> items1 = new ArrayList<>();
        List<Integer> items2 = new ArrayList<>();

        Observable<Integer> source = Observable.<Integer>create((observer, disposable) -> {
            observer.onNext(1);
            observer.onNext(2);
            observer.onNext(3);
            observer.onComplete();
        });

        source.subscribe((Integer item) -> items1.add(item));
        source.subscribe((Integer item) -> items2.add(item));

        assertThat(items1).containsExactly(1, 2, 3);
        assertThat(items2).containsExactly(1, 2, 3);
    }

    @Test
    void testEmptyObservable() {
        List<Integer> items = new ArrayList<>();

        Observable<Integer> source = Observable.<Integer>create((observer, disposable) -> {
            observer.onComplete();
        });

        source.subscribe((Integer item) -> items.add(item));

        assertThat(items).isEmpty();
    }

    @Test
    void testComplexFlatMapChain() {
        List<String> results = new ArrayList<>();

        Observable<Integer> source = Observable.<Integer>create((observer, disposable) -> {
            observer.onNext(1);
            observer.onNext(2);
            observer.onComplete();
        });

        source.flatMap((Integer x) -> {
                    return Observable.<Integer>create((inner, d) -> {
                        inner.onNext(x);
                        inner.onNext(x + 10);
                        inner.onComplete();
                    });
                }).<String>map(x -> "Value: " + x)
                .subscribe((String item) -> results.add(item));

        assertThat(results).containsExactly("Value: 1", "Value: 11", "Value: 2", "Value: 12");
    }
}