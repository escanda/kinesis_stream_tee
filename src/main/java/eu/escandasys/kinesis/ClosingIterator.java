package eu.escandasys.kinesis;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Stream;

import com.google.common.collect.ForwardingIterator;

public class ClosingIterator<U> extends ForwardingIterator<U> implements Closeable {
    public static <U> ClosingIterator<U> of(Supplier<Optional<U>> supplier, Runnable closer) {
        return new ClosingIterator<>(supplier, closer);
    }

    private final Runnable closer;
    private final Iterator<U> delegate;
    
    public ClosingIterator(Supplier<Optional<U>> supplier, Runnable closer) {
        this.delegate = Stream.generate(supplier)
            .flatMap(t -> t.stream())
            .takeWhile(Predicate.not(Objects::isNull))
            .iterator();
        this.closer = closer;
    }

    @Override
    public void close() throws IOException {
        closer.run();
    }

    @Override
    protected Iterator<U> delegate() {
        return delegate;
    }
}
