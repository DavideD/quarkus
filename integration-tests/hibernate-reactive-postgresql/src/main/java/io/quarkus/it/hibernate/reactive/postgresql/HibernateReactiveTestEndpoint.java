package io.quarkus.it.hibernate.reactive.postgresql;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;

import org.hibernate.reactive.mutiny.Mutiny;

import io.quarkus.security.Authenticated;
import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.pgclient.PgPool;
import io.vertx.mutiny.sqlclient.Row;
import io.vertx.mutiny.sqlclient.RowSet;
import io.vertx.mutiny.sqlclient.Tuple;

@Path("/tests")
@Authenticated
public class HibernateReactiveTestEndpoint {

    @Inject
    Uni<Mutiny.Session> sessionUni;

    // Injecting a Vert.x Pool is not required, it's only used to
    // independently validate the contents of the database for the test
    @Inject
    PgPool pgPool;

    @GET
    @Path("/reactiveFindMutiny")
    public Uni<GuineaPig> reactiveFindMutiny() {
        final GuineaPig expectedPig = new GuineaPig(5, "Aloi");
        return populateDB().chain(() -> sessionUni.chain(session -> session.find(GuineaPig.class, expectedPig.getId())));
    }

    @GET
    @Path("/reactivePersist")
    public Uni<String> reactivePersist() {
        final GuineaPig pig = new GuineaPig(10, "Tulip");
        return sessionUni
                .call(session -> session.persist(pig))
                .chain(Mutiny.Session::flush)
                .chain(() -> selectNameFromId(10));
    }

    @GET
    @Path("/reactiveCowPersist")
    public Uni<FriesianCow> reactiveCowPersist() {
        final FriesianCow cow = new FriesianCow();
        cow.name = "Carolina";
        return sessionUni
                .call(session -> session.persist(cow))
                .call(Mutiny.Session::flush)
                .chain(s -> s.createQuery("from FriesianCow f where f.name = :name", FriesianCow.class)
                        .setParameter("name", cow.name).getSingleResult());
    }

    @GET
    @Path("/reactiveRemoveTransientEntity")
    public Uni<String> reactiveRemoveTransientEntity() {
        return populateDB()
                .chain(() -> selectNameFromId(5))
                .invoke(name -> {
                    if (name == null)
                        throw new AssertionError("Database was not populated properly");
                })
                .chain(() -> sessionUni.call(session -> session.merge(new GuineaPig(5, "Aloi"))
                        .chain(session::remove))
                        .call(Mutiny.Session::flush)
                        .chain(() -> selectNameFromId(5))
                        .map(result -> result == null ? "OK" : result));
    }

    @GET
    @Path("/reactiveRemoveManagedEntity")
    public Uni<String> reactiveRemoveManagedEntity() {
        return populateDB()
                .chain(() -> selectNameFromId(5))
                .invoke(name -> {
                    if (name == null)
                        throw new AssertionError("Database was not populated properly");
                })
                .chain(() -> sessionUni.call(session -> session.find(GuineaPig.class, 5)
                        .chain(session::remove))
                        .call(Mutiny.Session::flush)
                        .chain(() -> selectNameFromId(5))
                        .map(result -> result == null ? "OK" : result));
    }

    @GET
    @Path("/reactiveUpdate")
    public Uni<String> reactiveUpdate() {
        final String NEW_NAME = "Tina";
        return populateDB()
                .chain(() -> sessionUni.call(session -> session.find(GuineaPig.class, 5)
                        .invoke(pig -> {
                            if (NEW_NAME.equals(pig.getName()))
                                throw new AssertionError("Pig already had name " + NEW_NAME);
                            pig.setName(NEW_NAME);
                        }))
                        .call(Mutiny.Session::flush)
                        .chain(() -> selectNameFromId(5)));
    }

    private Uni<RowSet<Row>> populateDB() {
        return Uni.combine().all().unis(
                pgPool.query("DELETE FROM Pig").execute(),
                pgPool.query("DELETE FROM Cow").execute())
                .asTuple()
                .chain(() -> pgPool.preparedQuery("INSERT INTO Pig (id, name) VALUES (5, 'Aloi')").execute());
    }

    private Uni<String> selectNameFromId(Integer id) {
        return pgPool.preparedQuery("SELECT name FROM Pig WHERE id = $1").execute(Tuple.of(id)).map(rowSet -> {
            if (rowSet.size() == 1) {
                return rowSet.iterator().next().getString(0);
            } else if (rowSet.size() > 1) {
                throw new AssertionError("More than one result returned: " + rowSet.size());
            } else {
                return null; // Size 0
            }
        });
    }

}
