package io.quarkus.hibernate.reactive.runtime;

import java.util.concurrent.CompletableFuture;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.context.RequestScoped;
import javax.enterprise.inject.Disposes;
import javax.enterprise.inject.Produces;
import javax.inject.Inject;

import org.hibernate.reactive.common.spi.Implementor;
import org.hibernate.reactive.context.Context;
import org.hibernate.reactive.context.impl.BaseKey;
import org.hibernate.reactive.mutiny.Mutiny;

import io.quarkus.arc.DefaultBean;
import io.smallrye.mutiny.Uni;

@ApplicationScoped
public class ReactiveSessionProducer {

    private final Mutiny.SessionFactory mutinySessionFactory;
    private final Context context;
    private final BaseKey<Mutiny.Session> contextKeyForSession;

    @Inject
    public ReactiveSessionProducer(Mutiny.SessionFactory mutinySessionFactory) {
        this.mutinySessionFactory = mutinySessionFactory;
        context = ((Implementor) mutinySessionFactory).getContext();
        contextKeyForSession = new BaseKey<>(Mutiny.Session.class, ((Implementor) mutinySessionFactory).getUuid());
    }

    @Produces
    @RequestScoped
    @DefaultBean
    public Uni<Mutiny.Session> createMutinySession() {
        Mutiny.Session current = context.get(contextKeyForSession);
        if (current != null && current.isOpen()) {
            return Uni.createFrom().item(current);
        } else {
            return mutinySessionFactory.openSession().invoke(session -> context.put(contextKeyForSession, session));
        }
    }

    public void disposeMutinySession(@Disposes Uni<Mutiny.Session> ignore) {
        final Mutiny.Session session = context.get(contextKeyForSession);
        if (session != null) {
            context.remove(contextKeyForSession);
            if (session.isOpen()) {
                // N.B. make sure to subscribe as this is a Mutiny based Session:
                // operations don't happen at all if there is no subscription.
                final CompletableFuture<Void> closeOperation = session.close()
                        .subscribeAsCompletionStage();
                if (!io.vertx.core.Context.isOnVertxThread()) {
                    //When invoked from blocking code, behave as expected and block on the operation
                    //so to not starve resources with a deferred close.
                    closeOperation.join();
                }
                // [else] no need to block. Worst that can happen is that the opening
                // of a new Mutiny.Session needs to wait for an available connection,
                // which implicitly orders it as "downstream" from the previous close
                // to have actually happened as the connection pool is reactive.
                // Also, if connections are available there is no real need to wait for
                // it, so this should be good.
            }
        }
    }

    public Mutiny.Session getCurrentSessionIfOpen() {
        final Mutiny.Session session = context.get(contextKeyForSession);
        return session.isOpen() ? session : null;
    }

}
