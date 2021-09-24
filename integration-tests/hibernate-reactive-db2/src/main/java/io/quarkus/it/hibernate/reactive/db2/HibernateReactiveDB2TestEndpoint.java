package io.quarkus.it.hibernate.reactive.db2;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;

import org.hibernate.reactive.mutiny.Mutiny;

import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.db2client.DB2Pool;
import io.vertx.mutiny.sqlclient.Row;
import io.vertx.mutiny.sqlclient.RowSet;
import io.vertx.mutiny.sqlclient.Tuple;

@Path("/tests")
public class HibernateReactiveDB2TestEndpoint {

    @Inject
    Uni<Mutiny.Session> uniSession;

    // Injecting a Vert.x Pool is not required, it's only used to
    // independently validate the contents of the database for the test
    @Inject
    DB2Pool db2Pool;

    @GET
    @Path("/reactiveFindMutiny")
    public Uni<GuineaPig> reactiveFindMutiny() {
        final GuineaPig expectedPig = new GuineaPig(5, "Aloi");
        return populateDB().chain(() -> uniSession
                .chain(mutinySession -> mutinySession
                        .find(GuineaPig.class, expectedPig.getId())));
    }

    @GET
    @Path("/reactivePersist")
    public Uni<String> reactivePersist() {
        return uniSession
                .chain(mutinySession -> mutinySession.persist(new GuineaPig(10, "Tulip"))
                        .chain(mutinySession::flush))
                .chain(() -> selectNameFromId(10));
    }

    @GET
    @Path("/reactiveRemoveTransientEntity")
    public Uni<String> reactiveRemoveTransientEntity() {
        return populateDB()
                .chain(() -> selectNameFromId(5))
                .map(name -> {
                    if (name == null) {
                        throw new AssertionError("Database was not populated properly");
                    }
                    return name;
                })
                .chain(() -> uniSession.chain(mutinySession -> mutinySession
                        .merge(new GuineaPig(5, "Aloi"))
                        .chain(mutinySession::remove)
                        .chain(mutinySession::flush)))
                .chain(() -> selectNameFromId(5))
                .onItem().ifNotNull().transform(result -> result)
                .onItem().ifNull().continueWith("OK");
    }

    @GET
    @Path("/reactiveRemoveManagedEntity")
    public Uni<String> reactiveRemoveManagedEntity() {
        return populateDB()
                .chain(() -> uniSession)
                .chain(mutinySession -> mutinySession
                        .find(GuineaPig.class, 5)
                        .chain(mutinySession::remove)
                        .chain(mutinySession::flush))
                .chain(() -> selectNameFromId(5))
                .onItem().ifNotNull().transform(result -> result)
                .onItem().ifNull().continueWith("OK");
    }

    @GET
    @Path("/reactiveUpdate")
    public Uni<String> reactiveUpdate() {
        final String NEW_NAME = "Tina";
        return populateDB()
                .chain(() -> uniSession)
                .chain(mutinySession -> mutinySession
                        .find(GuineaPig.class, 5)
                        .invoke(pig -> {
                            if (NEW_NAME.equals(pig.getName())) {
                                throw new AssertionError("Pig already had name " + NEW_NAME);
                            }
                            pig.setName(NEW_NAME);
                        })
                        .chain(mutinySession::flush))
                .chain(() -> selectNameFromId(5));
    }

    private Uni<RowSet<Row>> populateDB() {
        return db2Pool.query("DELETE FROM Pig").execute()
                .chain(() -> db2Pool.preparedQuery("INSERT INTO Pig (id, name) VALUES (5, 'Aloi')").execute());
    }

    private Uni<String> selectNameFromId(Integer id) {
        return db2Pool.preparedQuery("SELECT name FROM Pig WHERE id = ?").execute(Tuple.of(id)).map(rowSet -> {
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
