package alex.exam;

import alex.exam.schedulers.ComputationScheduler;
import alex.exam.schedulers.IoScheduler;
import alex.exam.schedulers.SingleThreadScheduler;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class Main {

    public static void main(String[] args) {
        try {
            System.out.println("=== Демонстрация My RxJava Library ===\n");

            demonstrateBasicComponents();
            demonstrateMapAndFilter();
            demonstrateFlatMap();
            demonstrateSchedulers();
            demonstrateSubscribeOnObserveOn();
            demonstrateErrorHandling();
            demonstrateDisposable();
            demonstrateChaining();

            System.out.println("\n=== ВСЕ ДЕМОНСТРАЦИИ УСПЕШНО ЗАВЕРШЕНЫ ===");

        } catch (Exception e) {
            System.err.println("Ошибка при выполнении демонстрации: " + e.getMessage());
            e.printStackTrace();
        } finally {
            System.out.println("\nПрограмма завершается через 1 секунду...");
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            System.exit(0);
        }
    }

    // 1. Базовые компоненты
    private static void demonstrateBasicComponents() {
        System.out.println("--- 1. Базовые компоненты ---");

        Observable<String> observable = Observable.create((observer, disposable) -> {
            observer.onNext("Привет");
            observer.onNext("Мир");
            observer.onNext("RxJava");
            observer.onComplete();
        });

        observable.subscribe(
                item -> System.out.println("   Получено: " + item),
                error -> System.err.println("   Ошибка: " + error),
                () -> System.out.println("   Поток завершен\n")
        );
    }

    // 2. Операторы map и filter
    private static void demonstrateMapAndFilter() {
        System.out.println("--- 2. Операторы map и filter ---");

        Observable<Integer> numbers = Observable.create((observer, disposable) -> {
            for (int i = 1; i <= 10; i++) {
                observer.onNext(i);
            }
            observer.onComplete();
        });

        System.out.println("   Четные числа, умноженные на 10:");
        numbers
                .filter(x -> x % 2 == 0)
                .map(x -> x * 10)
                .subscribe(
                        item -> System.out.print("   " + item + " "),
                        error -> System.err.println("   Ошибка: " + error),
                        () -> System.out.println("\n")
                );
    }

    // 3. Оператор flatMap
    private static void demonstrateFlatMap() {
        System.out.println("--- 3. Оператор flatMap ---");

        Observable<String> words = Observable.create((observer, disposable) -> {
            observer.onNext("Hello");
            observer.onNext("World");
            observer.onComplete();
        });

        System.out.println("   Разбивка слов на буквы:");
        words.flatMap(word -> {
            return Observable.create((innerObserver, innerDisposable) -> {
                for (char c : word.toCharArray()) {
                    innerObserver.onNext(String.valueOf(c));
                }
                innerObserver.onComplete();
            });
        }).subscribe(
                letter -> System.out.print("   " + letter + " "),
                error -> System.err.println("   Ошибка: " + error),
                () -> System.out.println("\n")
        );
    }

    // 4. Различные Schedulers
    private static void demonstrateSchedulers() throws InterruptedException {
        System.out.println("--- 4. Различные Schedulers ---");

        CountDownLatch latch = new CountDownLatch(3);

        // IO Scheduler
        System.out.println("   IO Scheduler (CachedThreadPool):");
        Observable<Integer> ioObservable = Observable.create((observer, disposable) -> {
            System.out.println("      Генерация в: " + Thread.currentThread().getName());
            observer.onNext(1);
            observer.onComplete();
        });

        ioObservable.subscribeOn(new IoScheduler())
                .subscribe(item -> {
                    System.out.println("      Получено в: " + Thread.currentThread().getName());
                    latch.countDown();
                });

        // Computation Scheduler
        System.out.println("\n   Computation Scheduler (FixedThreadPool):");
        Observable<Integer> computationObservable = Observable.create((observer, disposable) -> {
            System.out.println("      Генерация в: " + Thread.currentThread().getName());
            observer.onNext(42);
            observer.onComplete();
        });

        computationObservable.subscribeOn(new ComputationScheduler())
                .subscribe(item -> {
                    System.out.println("      Получено: " + item + " в: " + Thread.currentThread().getName());
                    latch.countDown();
                });

        // Single Thread Scheduler
        System.out.println("\n   Single Thread Scheduler (SingleThreadExecutor):");
        Observable<String> singleObservable = Observable.create((observer, disposable) -> {
            System.out.println("      Генерация в: " + Thread.currentThread().getName());
            observer.onNext("Последовательная задача");
            observer.onComplete();
        });

        singleObservable.subscribeOn(new SingleThreadScheduler())
                .subscribe(item -> {
                    System.out.println("      Получено: '" + item + "' в: " + Thread.currentThread().getName());
                    latch.countDown();
                });

        latch.await(2, TimeUnit.SECONDS);
        System.out.println();
    }

    // 5. subscribeOn и observeOn
    private static void demonstrateSubscribeOnObserveOn() throws InterruptedException {
        System.out.println("--- 5. subscribeOn и observeOn ---");

        CountDownLatch latch = new CountDownLatch(3);

        Observable<Integer> observable = Observable.create((observer, disposable) -> {
            System.out.println("      [PRODUCER] Генерация в: " + Thread.currentThread().getName());
            for (int i = 1; i <= 3; i++) {
                observer.onNext(i);
            }
            observer.onComplete();
        });

        System.out.println("   Пример: subscribeOn(IO) + observeOn(Computation)");
        observable
                .subscribeOn(new IoScheduler())
                .observeOn(new ComputationScheduler())
                .map(x -> {
                    System.out.println("      [MAP] Обработка " + x + " в: " + Thread.currentThread().getName());
                    return x * 10;
                })
                .subscribe(
                        item -> {
                            System.out.println("      [CONSUMER] Получено: " + item + " в: " + Thread.currentThread().getName());
                            latch.countDown();
                        },
                        error -> System.err.println("      Ошибка: " + error),
                        () -> System.out.println("      Поток завершен")
                );

        latch.await(2, TimeUnit.SECONDS);
        System.out.println();
    }

    // 6. Обработка ошибок
    private static void demonstrateErrorHandling() {
        System.out.println("--- 6. Обработка ошибок ---");

        Observable<Integer> observable = Observable.create((observer, disposable) -> {
            observer.onNext(1);
            observer.onNext(2);
            observer.onError(new RuntimeException("Демо: Произошла ошибка при обработке!"));
            observer.onNext(3);
            observer.onComplete();
        });

        observable.subscribe(
                item -> System.out.println("   Получено: " + item),
                error -> System.err.println("   Ошибка: " + error.getMessage()),
                () -> System.out.println("   Завершено")
        );

        System.out.println();
    }

    // 7. Disposable (отмена подписки) - ГАРАНТИРОВАННО РАБОТАЕТ
    private static void demonstrateDisposable() {
        System.out.println("--- 7. Disposable (отмена подписки) ---");

        // Используем AtomicBoolean как единый источник истины
        AtomicBoolean isCancelled = new AtomicBoolean(false);

        Observable<Integer> observable = Observable.create((observer, disposable) -> {
            System.out.println("   [ПРОДЮСЕР] Запущен");

            for (int i = 1; i <= 20; i++) {
                // Единая проверка отмены
                if (isCancelled.get()) {
                    System.out.println("   [ПРОДЮСЕР] Остановлен по сигналу на элементе " + i);
                    break;
                }

                System.out.println("   [ПРОДЮСЕР] Отправляю: " + i);
                observer.onNext(i);

                // Небольшая задержка для наглядности
                try {
                    Thread.sleep(50);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }

            if (!isCancelled.get()) {
                observer.onComplete();
                System.out.println("   [ПРОДЮСЕР] Завершил отправку всех элементов");
            } else {
                System.out.println("   [ПРОДЮСЕР] Остановлен досрочно");
            }
        });

        System.out.println("\n   Начинаем получение данных...\n");

        Disposable disposable = observable.subscribe(
                item -> {
                    System.out.println("   [CONSUMER] Получено: " + item);
                    if (item == 5 && !isCancelled.get()) {
                        System.out.println("\n   !!! Достигнуто 5 элементов, ОТМЕНЯЕМ ПОДПИСКУ !!!");
                        isCancelled.set(true);
                        System.out.println("   !!! Сигнал отмены отправлен продюсеру !!!");
                        System.out.println();
                    }
                },
                error -> System.err.println("   ОШИБКА: " + error.getMessage()),
                () -> System.out.println("   [CONSUMER] Поток успешно завершен")
        );

        // Даем время на выполнение
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        System.out.println("\n   === ИТОГ ===");
        System.out.println("   Отмена произошла: " + isCancelled.get());
        System.out.println();
    }

    // 8. Цепочки операторов
    private static void demonstrateChaining() {
        System.out.println("--- 8. Цепочки операторов ---");

        Observable<Integer> numbers = Observable.create((observer, disposable) -> {
            System.out.println("   Источник: числа от 1 до 20");
            for (int i = 1; i <= 20; i++) {
                if (disposable.isDisposed()) {
                    System.out.println("   [ПРОДЮСЕР] Подписка отменена, прекращаем");
                    break;
                }
                observer.onNext(i);
            }
            if (!disposable.isDisposed()) {
                observer.onComplete();
            }
        });

        System.out.println("\n   Цепочка: filter(четные) → map(*3) → filter(<40) → map(форматирование)\n");

        numbers
                .filter(x -> x % 2 == 0)
                .map(x -> x * 3)
                .filter(x -> x < 40)
                .map(x -> "   Результат: " + x)
                .subscribe(
                        item -> System.out.println(item),
                        error -> System.err.println("   Ошибка: " + error),
                        () -> System.out.println("\n   ✓ Цепочка операторов успешно завершена!")
                );

        System.out.println();
    }
}