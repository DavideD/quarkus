package io.quarkus.arc.test.contexts.request.propagation;

import io.quarkus.arc.Arc;
import io.quarkus.arc.InstanceHandle;
import io.quarkus.arc.test.ArcTestContainer;
import io.smallrye.mutiny.Uni;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.context.RequestScoped;
import javax.enterprise.context.control.ActivateRequestContext;
import javax.enterprise.inject.Disposes;
import javax.enterprise.inject.Produces;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

public class ActivateRequestContextProducerTest {

    @RegisterExtension
    public ArcTestContainer container = new ArcTestContainer(FakeSessionProducer.class, SessionClient.class);

    @Test
    public void testAsyncProducer() throws InterruptedException, ExecutionException {
        FakeSession.CLOSED.set(false);

        InstanceHandle<SessionClient> clientHandler = Arc.container().instance(SessionClient.class);
        Assertions.assertFalse(FakeSession.isClosed());
        String result = clientHandler.get().openSession().await().indefinitely();
        clientHandler.close();
        clientHandler.destroy();

        Assertions.assertEquals("Opened!", result);
        Assertions.assertTrue(FakeSession.isClosed());
    }

    @ApplicationScoped
    static class SessionClient {
        @Inject
        FakeSession session;

        @ActivateRequestContext
        public Uni<String> openSession() {
            return Uni.createFrom()
                    .item(() -> session)
                    .map(FakeSession::open);
        }
    }

    static class FakeSession {
        public static final AtomicBoolean CLOSED = new AtomicBoolean(false);

        public String open() {
            CLOSED.set(false);
            return "Opened!";
        }

        public String close() {
            CLOSED.set(true);
            return "Closed!";
        }

        public static boolean isClosed() {
            return CLOSED.get();
        }
    }

    @Singleton
    static class FakeSessionProducer {

        @Produces
        @RequestScoped
        FakeSession produceSession() {
            return new FakeSession();
        }

        void disposeLong(@Disposes FakeSession session) {
            session.close();
        }
    }

}
