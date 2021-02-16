package it.ness.reactiveapi;

import javax.inject.Singleton;
import javax.ws.rs.Consumes;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import io.quarkus.hibernate.reactive.panache.PanacheEntityBase;
import io.quarkus.hibernate.reactive.panache.PanacheQuery;
import io.quarkus.panache.common.Parameters;
import io.quarkus.panache.common.Sort;
import io.smallrye.mutiny.Uni;
import it.ness.api.service.RsRepositoryServiceV3Reactive;
import it.ness.reactiveapi.model.Fruit;

@Path("/fruits")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@Singleton
public class FruitServiceRs extends RsRepositoryServiceV3Reactive<Fruit, String> {

    public FruitServiceRs() {
        super(Fruit.class);
    }

    @Override
    protected String getDefaultOrderBy() {
        return "name asc";
    }

    @Override
    public Uni<PanacheQuery<PanacheEntityBase>> getSearch(String orderBy) {
        return Uni.createFrom().item(sort(orderBy))
                .onItem().transform(sort  -> sort != null
                        ? Fruit.find("select a from Fruit a", sort)
                        : Fruit.find("select a from Fruit a"))
                .onItem().transform(search -> {
                    if (nn("like.name")) {
                        search.filter("like.name", Parameters.with("name", likeParamToLowerCase("like.name")));
                    }
                    return search;
                });
    }

    // Check the uniqueness of the item, throws an exception if the item is already in the db
    @Override
    protected Uni<Fruit> prePersist(Fruit fruit) {
        return Fruit.isNew(fruit.name)
                .onItem().transform(unique -> {
                    if (unique) return Uni.createFrom().item(true);
                    else {
                        throw new RuntimeException("Item already present in db");
                    }
                })
                .onItem().transform(i -> fruit);
    }
}
