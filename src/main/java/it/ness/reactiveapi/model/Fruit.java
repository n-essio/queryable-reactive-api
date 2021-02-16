package it.ness.reactiveapi.model;

import io.quarkus.hibernate.reactive.panache.PanacheEntityBase;
import io.smallrye.mutiny.Uni;
import org.hibernate.annotations.Filter;
import org.hibernate.annotations.FilterDef;
import org.hibernate.annotations.GenericGenerator;
import org.hibernate.annotations.ParamDef;

import javax.persistence.*;
import java.util.List;

@Entity
@Table(name = "fruits")

@FilterDef(name = "like.name", parameters = @ParamDef(name = "name", type = "string"))
@Filter(name = "like.name", condition = "lower(name) LIKE :name")

public class Fruit extends PanacheEntityBase {

    @GeneratedValue(generator = "uuid")
    @GenericGenerator(name = "uuid", strategy = "uuid2")
    @Column(name = "uuid", unique = true)
    @Id
    public String uuid;
    public String name;

    public static Uni<List<Fruit>> getListWithName(String name) {
        return Fruit.find("name", name).list();
    }

    public static Uni<Boolean> isNew(String name) {
        return getListWithName(name)
                .onItem().transform(List::isEmpty);
    }
}
