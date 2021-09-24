package io.quarkus.it.hibernate.reactive.mysql;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;

import org.hibernate.reactive.mutiny.Mutiny;

import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.mysqlclient.MySQLPool;
import io.vertx.mutiny.sqlclient.Row;
import io.vertx.mutiny.sqlclient.RowSet;
import io.vertx.mutiny.sqlclient.Tuple;

@Path("/alternative-tests")
public class HibernateReactiveMySQLAlternativeTestEndpoint {

    @Inject
    Uni<Mutiny.Session> mutinySession;

    // Injecting a Vert.x Pool is not required, it us only used to
    // independently validate the contents of the database for the test
    @Inject
    MySQLPool mysqlPool;

    @GET
    @Path("/reactiveFindMutiny")
    public Uni<GuineaPig> reactiveFindMutiny() {
        final GuineaPig expectedPig = new GuineaPig(5, "Aloi");
        return populateDB()
                .chain(() -> mutinySession.chain(session -> session.find(GuineaPig.class, expectedPig.getId())));
    }

    @GET
    @Path("/reactivePersist")
    public Uni<String> reactivePersist() {
        return mutinySession
                .chain(session -> session.persist(new GuineaPig(10, "Tulip"))
                        .chain(session::flush))
                .chain(() -> selectNameFromId(10));
    }

    @GET
    @Path("/reactiveRemoveTransientEntity")
    public Uni<String> reactiveRemoveTransientEntity() {
        return populateDB()
                .chain(() -> selectNameFromId(5))
                .invoke(name -> {
                    if (name == null) {
                        throw new AssertionError("Database was not populated properly");
                    }
                })
                .chain(() -> mutinySession.chain(session -> session.merge(new GuineaPig(5, "Aloi"))
                        .chain(session::remove)
                        .chain(session::flush)))
                .chain(() -> selectNameFromId(5))
                .onItem().ifNotNull().transform(result -> result)
                .onItem().ifNull().continueWith("OK");
    }

    @GET
    @Path("/reactiveRemoveManagedEntity")
    public Uni<String> reactiveRemoveManagedEntity() {
        return populateDB()
                .chain(() -> mutinySession.chain(session -> session.find(GuineaPig.class, 5)
                        .chain(session::remove)
                        .chain(session::flush)))
                .chain(() -> selectNameFromId(5))
                .onItem().ifNotNull().transform(result -> result)
                .onItem().ifNull().continueWith("OK");
    }

    @GET
    @Path("/reactiveUpdate")
    public Uni<String> reactiveUpdate() {
        final String NEW_NAME = "Tina";
        return populateDB()
                .chain(() -> mutinySession.chain(session -> session.find(GuineaPig.class, 5)
                        .invoke(pig -> {
                            if (NEW_NAME.equals(pig.getName())) {
                                throw new AssertionError("Pig already had name " + NEW_NAME);
                            }
                            pig.setName(NEW_NAME);
                        })
                        .chain(session::flush)))
                .chain(() -> selectNameFromId(5));
    }

    private Uni<RowSet<Row>> populateDB() {
        return mysqlPool.query("DELETE FROM Pig").execute()
                .flatMap(junk -> mysqlPool.preparedQuery("INSERT INTO Pig (id, name) VALUES (5, 'Aloi')").execute());
    }

    private Uni<String> selectNameFromId(Integer id) {
        return mysqlPool.preparedQuery("SELECT name FROM Pig WHERE id = ?").execute(Tuple.of(id)).map(rowSet -> {
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
