package alex.exam;

import alex.exam.operators.FilterOperator;
import alex.exam.operators.FlatMapOperator;
import alex.exam.operators.MapOperator;
import alex.exam.schedulers.ComputationScheduler;
import alex.exam.subscription.Subscription;

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

public abstract class Observable<T> {

    public interface OnSubscribe<T> {
        void subscribe(Observer<? super T> observer);
    }

    private final OnSubscribe<T> onSubscribe;

    protected Observable(OnSubscribe<T> onSubscribe) {
        this.onSubscribe = onSubscribe;
    }

    // Создание Observable через create()
    public static <T> Observable<T> create(OnSubscribe<T> onSubscribe) {
        return new Observable<>(onSubscribe) {};
    }

    // Подписка с полным Observer
    public Disposable subscribe(Observer<T> observer) {
        Subscription subscription = new Subscription();
        onSubscribe.subscribe(new Observer<T>() {
            @Override
            public void onNext(T item) {
                if (!subscription.isDisposed()) {
                    observer.onNext(item);
                }
            }

            @Override
            public void onError(Throwable t) {
                if (!subscription.isDisposed()) {
                    observer.onError(t);
                }
            }

            @Override
            public void onComplete() {
                if (!subscription.isDisposed()) {
                    observer.onComplete();
                }
            }
        });
        return subscription;
    }

    // Упрощенная подписка
    public Disposable subscribe(Consumer<T> onNext, Consumer<Throwable> onError, Runnable onComplete) {
        return subscribe(new Observer<T>() {
            @Override
            public void onNext(T item) { onNext.accept(item); }
            @Override
            public void onError(Throwable t) { onError.accept(t); }
            @Override
            public void onComplete() { onComplete.run(); }
        });
    }

    public Disposable subscribe(Consumer<T> onNext) {
        return subscribe(onNext, Throwable::printStackTrace, () -> {});
    }

    // Оператор map
    public <R> Observable<R> map(Function<T, R> mapper) {
        return new MapOperator<>(this, mapper);
    }

    // Оператор filter
    public Observable<T> filter(Predicate<T> predicate) {
        return new FilterOperator<>(this, predicate);
    }

    // Оператор flatMap
    public <R> Observable<R> flatMap(Function<T, Observable<R>> mapper) {
        return new FlatMapOperator<>(this, mapper);
    }

    // Управление потоками
    public Observable<T> subscribeOn(Scheduler scheduler) {
        return create(observer -> {
            scheduler.execute(() -> onSubscribe.subscribe(observer));
        });
    }

    public Observable<T> observeOn(Scheduler scheduler) {
        return create(observer -> {
            onSubscribe.subscribe(new Observer<T>() {
                @Override
                public void onNext(T item) {
                    scheduler.execute(() -> observer.onNext(item));
                }

                @Override
                public void onError(Throwable t) {
                    scheduler.execute(() -> observer.onError(t));
                }

                @Override
                public void onComplete() {
                    scheduler.execute(observer::onComplete);
                }
            });
        });
    }
}