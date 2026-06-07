package alex.exam.subscription;

import alex.exam.Disposable;

public class Subscription implements Disposable {
    private volatile boolean disposed = false;

    @Override
    public void dispose() {
        disposed = true;
    }

    @Override
    public boolean isDisposed() {
        return disposed;
    }
}
