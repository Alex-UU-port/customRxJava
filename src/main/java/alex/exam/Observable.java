package alex.exam;

import alex.exam.operators.FilterOperator;
import alex.exam.operators.FlatMapOperator;
import alex.exam.operators.MapOperator;
import alex.exam.subscription.Subscription;

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

public abstract class Observable<T> {

    public interface OnSubscribe<T> {
        void subscribe(Observer<? super T> observer, Disposable disposable);
    }

    private final OnSubscribe<T> onSubscribe;

    protected Observable(OnSubscribe<T> onSubscribe) {
        this.onSubscribe = onSubscribe;
    }

    public static <T> Observable<T> create(OnSubscribe<T> onSubscribe) {
        return new Observable<T>(onSubscribe) {};
    }

    public Disposable subscribe(Observer<T> observer) {
        Subscription subscription = new Subscription();
        //System.out.println("   [Observable] Создан новый Subscription");

        // Сохраняем ссылку на subscription для доступа из лямбды
        final Subscription subRef = subscription;

        onSubscribe.subscribe(new Observer<T>() {
            @Override
            public void onNext(T item) {
                if (!subRef.isDisposed()) {
                    observer.onNext(item);
                } else {
                    System.out.println("   [Observer-wrapper] Пропускаем onNext, подписка отменена");
                }
            }

            @Override
            public void onError(Throwable t) {
                if (!subRef.isDisposed()) {
                    observer.onError(t);
                }
            }

            @Override
            public void onComplete() {
                if (!subRef.isDisposed()) {
                    observer.onComplete();
                }
            }
        }, subRef);

        return subscription;
    }

    public Disposable subscribe(Consumer<? super T> onNext, Consumer<? super Throwable> onError, Runnable onComplete) {
        return subscribe(new Observer<T>() {
            @Override
            public void onNext(T item) { onNext.accept(item); }
            @Override
            public void onError(Throwable t) { onError.accept(t); }
            @Override
            public void onComplete() { onComplete.run(); }
        });
    }

    public Disposable subscribe(Consumer<? super T> onNext) {
        return subscribe(onNext, Throwable::printStackTrace, () -> {});
    }

    public <R> Observable<R> map(Function<? super T, ? extends R> mapper) {
        return new MapOperator<>(this, mapper);
    }

    public Observable<T> filter(Predicate<? super T> predicate) {
        return new FilterOperator<>(this, predicate);
    }

    public <R> Observable<R> flatMap(Function<T, Observable<R>> mapper) {
        return new FlatMapOperator<>(this, mapper);
    }

    public Observable<T> subscribeOn(Scheduler scheduler) {
        return create((observer, disposable) -> {
            scheduler.execute(() -> onSubscribe.subscribe(observer, disposable));
        });
    }

    public Observable<T> observeOn(Scheduler scheduler) {
        return create((observer, disposable) -> {
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
            }, disposable);
        });
    }
}