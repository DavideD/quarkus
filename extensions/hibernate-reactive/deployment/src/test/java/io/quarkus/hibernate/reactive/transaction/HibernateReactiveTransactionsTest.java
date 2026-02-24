package io.quarkus.hibernate.reactive.transaction;

import java.util.List;
import java.util.function.Function;

import org.hibernate.reactive.mutiny.Mutiny;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.quarkus.reactive.transaction.TransactionalInterceptorRequired;
import io.quarkus.test.QuarkusUnitTest;
import io.quarkus.test.vertx.RunOnVertxContext;
import io.quarkus.test.vertx.UniAsserter;
import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.sqlclient.Pool;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import org.assertj.core.api.Assertions;

import static org.assertj.core.api.Assertions.assertThat;

public class HibernateReactiveTransactionsTest {

    @RegisterExtension
    static final QuarkusUnitTest config = new QuarkusUnitTest()
            .withApplicationRoot(jar -> jar
                    .addClasses(Hero.class)
                    .addClasses(TransactionalInterceptorRequired.class)
                    .addAsResource("initialTransactionData.sql", "import.sql"))
            .withConfigurationResource("application-reactive-transaction.properties");

    @Inject
    Mutiny.SessionFactory sessionFactory;

    @Inject
    Pool pool;

    int initialPoolSize;

    @BeforeEach
    public void savePoolSize() {
        initialPoolSize = pool.size();
    }

    @AfterEach
    public void checkPollSize() {
        Assertions.assertThat(pool.size()).isEqualTo(initialPoolSize);
    }

    /**
     * This test shows how to use hibernate reactive .withTransaction to set transactional boundaries
     * Below there's testReactiveAnnotationTransaction which is the same test but with @Transactional
     *
     * @param asserter
     */
    @Test
    @RunOnVertxContext
    public void testReactiveManualTransaction(UniAsserter asserter) {
        // initialTransactionData.sql
        Long heroId = 50L;

        // First update, make sure it's committed
        asserter.assertThat(
                () -> sessionFactory.withTransaction(session -> updateHero(session, heroId, "updatedNameCommitted"))
                        // 2nd endpoint call
                        .chain(() -> sessionFactory.withTransaction(session -> session.find(Hero.class, heroId))),
                h -> assertThat(h.name).isEqualTo("updatedNameCommitted"));

        // Second update, make sure there's a rollback
        asserter.assertThat(
                () -> sessionFactory
                        .withTransaction(session -> updateHero(session, heroId, "this name won't appear")
                                .onItem().invoke(h -> {
                                    throw new RuntimeException("Failing update");
                                }))
                        .onFailure().recoverWithNull()
                        .chain(() -> sessionFactory.withTransaction(session -> session.find(Hero.class, heroId))),
                h -> assertThat(h.name).isEqualTo("updatedNameCommitted"));
    }

    @Inject
    Mutiny.Session session;

    @Test
    @RunOnVertxContext
    public void transactionalAnnotationPersistRollback(UniAsserter asserter) {
        // We want to insert a new hero, but an error occurs
        asserter.assertFailedWith(
                () -> transactional(session -> session
                        .persist(new Hero("Invincible"))
                        .invoke(h -> {
                            throw new RuntimeException("Oh NO! I cannot create the hero [" + h + "]");
                        })),
                t -> assertThat(t).hasMessageContaining("Oh NO!"));

        // Transaction should have been roll-backed, let's check the content of the db
        asserter.assertThat(this::selectHeroes, heroes -> assertThat(heroes)
                .extracting(Hero::getName)
                .containsExactly("initialName"));
    }

    @Test
    @RunOnVertxContext
    public void transactionalAnnotationPersistCommit(UniAsserter asserter) {
        Hero spalman = new Hero("Spalman");
        // A regular persist
        asserter.execute(() -> transactional(session -> session.persist(spalman)));

        asserter.assertThat(
                () -> transactional(s -> s.find(Hero.class, spalman.id)),
                hero -> assertThat(hero).extracting(Hero::getName).isEqualTo(spalman.name)
        );
    }

    @Test
    @RunOnVertxContext
    public void testReactiveAnnotationTransactionWithTwoMethods(UniAsserter asserter) {
        // initialTransactionData.sql
        Long heroId = 50L;

        asserter.assertThat(
                () -> updateCallingAnotherTransactionalMethod(heroId, "updatedNameTwiceCommitted")
                        .chain(() -> findHero(heroId)),
                h -> {
                    assertThat(h.name).isEqualTo("updatedNameTwiceCommitted");
                });

    }

    /**
     * Emulates the call to a method annotated with @{@link Transactional}
     * that uses the {@link org.hibernate.reactive.mutiny.Mutiny.Session}.
     * This way we don't have to create a new method everytime we want to add a test.
     */
    @Transactional
    public <T> Uni<T> transactional(Function<Mutiny.Session, Uni<T>> fun) {
        return fun.apply(session);
    }

    @Transactional
    public Uni<Hero> findHero(Long heroId) {
        return session.find(Hero.class, heroId);
    }

    @Transactional
    public Uni<Hero> updateWithCommit(Long heroId, String newName) {
        return updateHero(session, heroId, newName);
    }

    @Transactional
    public Uni<Hero> updateCallingAnotherTransactionalMethod(Long heroId, String newName) {
        return updateHero(session, heroId, newName + "thisShouldntAppear")
                .chain(h -> updateWithCommit(heroId, newName));
    }

    @Transactional
    public Uni<Hero> transactionalUpdateWithRollback(Long heroId, String newName) {
        return updateHero(session, heroId, newName)
                .onItem().invoke(h -> {
                    throw new RuntimeException("Failing update");
                });
    }

    @Transactional
    public Uni<Hero> transactionalPersistWithFailure(String newName) {
        return persistHero(session, newName)
                .onItem().invoke(h -> {
                    throw new RuntimeException("Oh NO! I cannot create the hero [" + h + "]");
                });
    }

    @Transactional
    public Uni<List<Hero>> selectHeroes() {
        return session.createSelectionQuery("from Hero", Hero.class).getResultList();
    }

    public Uni<Hero> persistHero(Mutiny.Session session, String newName) {
        Hero hero = new Hero();
        hero.setName(newName);
        return session
                .persist(hero)
                // In the real world, flushing is not required, but I want to run the query on the database
                // before rolling back
                .call(session::flush)
                .replaceWith(hero);
    }

    public Uni<Hero> updateHero(Mutiny.Session session, Long id, String newName) {
        return session.find(Hero.class, id)
                .map(h -> {
                    h.setName(newName);
                    return h;
                });
    }
}
