# Custom RxJava - Реализация реактивной библиотеки

## Архитектура системы

Система реализует основные концепции Reactive Streams с использованием паттерна "Наблюдатель".

### Ключевые компоненты

1. **Observable<T>** - источник событий, поддерживает подписку и операторы преобразования
2. **Observer<T>** - получатель событий с методами onNext, onError, onComplete
3. **Disposable** - управление подпиской, возможность отмены
4. **Scheduler** - управление потоками выполнения

### Принцип работы

- Observable создается через `create()` с передачей лямбды, которая эмитит данные
- Подписка осуществляется через `subscribe()` с передачей Observer
- Операторы (map, filter, flatMap) создают новый Observable, трансформирующий поток

## Schedulers и управление потоками

### Реализованные Scheduler

| Scheduler | Пул потоков | Применение |
|-----------|-------------|------------|
| **IoScheduler** | CachedThreadPool | I/O операции, работа с сетью, БД, файлами |
| **ComputationScheduler** | FixedThreadPool(CPU cores) | Вычисления, обработка данных |
| **SingleThreadScheduler** | SingleThreadExecutor | Последовательные задачи |

### Методы управления потоками

- `subscribeOn(Scheduler)` - задает поток для генерации событий
- `observeOn(Scheduler)` - задает поток для обработки событий

### Пример использования Schedulers

```java
Observable.create(observer -> {
    // Тяжелые вычисления в Computation
    observer.onNext(compute());
})
.subscribeOn(new ComputationScheduler())
.observeOn(new IoScheduler())
.subscribe(result -> saveToDatabase(result));
```

## Операторы
### Map
Преобразует каждый элемент потока

```java
observable.map(x -> x * 2)
          .subscribe(System.out::println);
```

### Filter
Фильтрует элементы по условию

```java
observable.filter(x -> x > 10)
          .subscribe(System.out::println);
```
### FlatMap
Разворачивает элементы в новые потоки

```java
observable.flatMap(id -> fetchUserData(id))
          .subscribe(user -> System.out.println(user));
```
### Обработка ошибок
Ошибки передаются в метод onError() Observer:

```java
observable.subscribe(
    item -> System.out.println(item),
    error -> System.err.println("Error: " + error),
    () -> System.out.println("Completed")
);
```
### Управление подпиской
```java
Disposable disposable = observable.subscribe(item -> process(item));

// Отмена подписки
if (someCondition) {
    disposable.dispose();
}
```
