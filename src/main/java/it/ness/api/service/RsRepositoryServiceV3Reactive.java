package it.ness.api.service;


import io.quarkus.hibernate.reactive.panache.PanacheEntityBase;
import io.quarkus.hibernate.reactive.panache.PanacheQuery;
import io.quarkus.panache.common.Page;
import io.quarkus.panache.common.Sort;
import io.quarkus.security.identity.SecurityIdentity;
import io.smallrye.mutiny.Uni;
import org.hibernate.reactive.mutiny.Mutiny;
import org.jboss.logging.Logger;
import org.jboss.resteasy.reactive.RestPath;

import javax.inject.Inject;
import javax.transaction.Transactional;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;


public abstract class RsRepositoryServiceV3Reactive<T extends PanacheEntityBase, U> extends RsResponseServiceReactive implements
        Serializable {

    @Inject
    Mutiny.Session mutinySession;

    private static final long serialVersionUID = 1L;
    protected Logger logger = Logger.getLogger(getClass());
    private Class<T> entityClass;

    public RsRepositoryServiceV3Reactive(Class<T> entityClass) {
        this.entityClass = entityClass;
    }

    public RsRepositoryServiceV3Reactive() { }

    protected Class<T> getEntityClass() {
        return entityClass;
    }

    protected SecurityIdentity getCurrentUser() {
        return null;
    }

    public Uni<T> find(U id) {
        return mutinySession.find(getEntityClass(), id);
    }

    protected abstract String getDefaultOrderBy();
    public abstract Uni<PanacheQuery<PanacheEntityBase>> getSearch(String orderBy);

    protected Uni<T> prePersist(T object) { return Uni.createFrom().item(object); }
    protected Uni<T> postPersist(T object) { return Uni.createFrom().item(object); }

    @POST
    public Uni<Response> persist(T object) {
        logger.info("persist");

        if (object == null) {
            logger.error("Failed to create resource: object is null");
            return getResponseUni(jsonErrorMessageResponse(null));
        }

        // PRE-PERSIST
        return prePersist(object)
                // PERSIST
                .onItem().transformToUni(i -> mutinySession.persist(object))
                // POST-PERSIST
                .onItem().transformToUni(i -> postPersist(object))

                // IF OK
                .chain(mutinySession::flush)
                .map(i -> Response.status(Response.Status.OK).entity(object).build())

                // ELSE
                .onFailure().recoverWithItem(f -> handleFailures(f, "Persist: "));
    }

    protected Uni<U> preFetch(U id) { return Uni.createFrom().item(id); }
    protected Uni<T> postFetch(T object) { return Uni.createFrom().item(object); }

    @GET
    @Path("/{id}")
    public Uni<Response> fetch(@RestPath("id") U id) {
        logger.info("fetch: " + id);

        return preFetch(id)
                .onItem().transformToUni(this::find)
                .onItem().transformToUni(this::postFetch)
                .onItem().ifNull().failWith(getObjectNotFoundException(id))
                .onItem().transform(item -> Response.status(Response.Status.OK).entity(item).build())
                .onFailure().recoverWithItem(f -> handleFailures(f, "fetch: "));

    }

    protected Uni<T> preUpdate(U id, T object) { return Uni.createFrom().item(object); }
    protected Uni<T> postUpdate(U id, T object) { return Uni.createFrom().item(object); }

    @PUT
    @Path("/{id}")
    public Uni<Response> update(@RestPath("id") U id, T object) {
        logger.info("update: " + id);

        return preUpdate(id, object)
                .onItem().transformToUni(i -> mutinySession.merge(object))
                .chain(mutinySession::flush)
                .onItem().transformToUni(i -> postUpdate(id, object))
                .onItem().transform(item -> Response.status(Response.Status.OK).entity(item).build())
                .onFailure().recoverWithItem(f -> handleFailures(f, "update: "));

    }

    protected Uni<U> preDelete(U id) { return Uni.createFrom().item(id); }
    public Uni<Void> toDelete(T object) { return mutinySession.remove(object); }
    protected Uni<U> postDelete(U id) { return Uni.createFrom().item(id); }

    @DELETE
    @Path("/{id}")
    public Uni<Response> delete(@RestPath("id") U id) {
        logger.info("delete: " + id);

        return preDelete(id)
                .onItem().transformToUni(this::find)
                .onItem().ifNull().failWith(getObjectNotFoundException(id))
                .onItem().transformToUni(this::toDelete)
                .chain(mutinySession::flush)
                .onItem().transformToUni(i -> postDelete(id))
                .onItem().transform(item -> jsonMessageResponse(Response.Status.NO_CONTENT, id))
                .onFailure().recoverWithItem(f -> handleFailures(f, "delete: "));

    }

    @GET
    @Path("/{id}/exist")
    public Uni<Response> exist(@RestPath("id") U id) {
        logger.info("exist: " + id);

        return find(id)
                .onItem().ifNull().failWith(getObjectNotFoundException(id))
                .onItem().transform(item -> jsonMessageResponse(Response.Status.OK, id))
                .onFailure().recoverWithItem(f -> handleFailures(f, "exist: "));
    }

    @GET
    @Path("/listSize")
    public Uni<Response> getListSize(@Context UriInfo ui) {
        logger.info("getListSize");

        return getSearch(null)
                .onItem().transformToUni(PanacheQuery::count)
                .onItem().transform(listSize ->
                        Response.status(Response.Status.OK).entity(listSize)
                            .header("Access-Control-Expose-Headers", "listSize")
                            .header("listSize", listSize).build())
                .onFailure().recoverWithItem(f -> handleFailures(f, "getListSize: "));

    }

    protected Uni<List<T>> postList(List<T> list) { return Uni.createFrom().item(list); }

    @GET
    public Uni<Response> getList(
            @DefaultValue("0") @QueryParam("startRow") Integer startRow,
            @DefaultValue("10") @QueryParam("pageSize") Integer pageSize,
            @QueryParam("orderBy") String orderBy, @Context UriInfo ui) {

        logger.info("getList");

        return getSearch(orderBy)
                .onItem().transformToUni(search -> getItemsList(search, pageSize, startRow))
                .onItem().transformToUni(this::postList)
                .onItem().transform(list ->
                        Response
                            .status(Response.Status.OK)
                            .entity(list)
                            .header("Access-Control-Expose-Headers", "startRow, pageSize, listSize")
                            .header("startRow", startRow)
                            .header("pageSize", pageSize)
                            .header("listSize", list.size())
                            .build())
                .onFailure().recoverWithItem(f -> handleFailures(f, "getList: "));
    }

    private Uni<List<T>> getItemsList(PanacheQuery<PanacheEntityBase> search, Integer pageSize, Integer startRow) {

        return search.count()
                .onItem().transformToUni(listSize -> {
                    if (listSize == 0) {
                        return Uni.createFrom().item(new ArrayList<T>());
                    } else {
                        int currentPage = 0;
                        int pSize = pageSize;
                        if (pSize != 0) {
                            currentPage = startRow / pSize;
                        } else {
                            pSize = listSize.intValue();
                        }
                        return search.page(Page.of(currentPage, pSize)).list();
                    }
                });
    }

    protected Response handleFailures(Throwable f, String whileDoing) {
        if (f instanceof ReactiveException) {
            ReactiveException e = (ReactiveException) f;
            return jsonMessageResponse(e.status, e.message);
        } else {
            logger.errorv(f, whileDoing);
            return jsonMessageResponse(Response.Status.BAD_REQUEST, f);
        }
    }

    protected ReactiveException getObjectNotFoundException(U id) {
        String errorMessage = String.format("Object [%s] with id [%s] not found",
                entityClass.getCanonicalName(), id);
        return new ReactiveException(errorMessage, Response.Status.NOT_FOUND);
    }

    protected Sort sort(String orderBy) throws RuntimeException {
        Sort sort = null;

        if (orderBy != null && !orderBy.trim().isEmpty()) {
            orderBy = orderBy.toLowerCase();
            if (orderBy.contains(",")) {
                String[] orderByClause = orderBy.split(",");
                for (String pz : orderByClause) {
                    sort = single(sort, pz);
                }
                return sort;
            } else {
                return single(null, orderBy);
            }
        }

        if (getDefaultOrderBy() != null && !getDefaultOrderBy().trim().isEmpty()) {
            if (getDefaultOrderBy().toLowerCase().contains("asc"))
                return Sort.by(getDefaultOrderBy().toLowerCase().replace("asc", "").trim()).ascending();
            if (getDefaultOrderBy().toLowerCase().contains("desc"))
                return Sort.by(getDefaultOrderBy().toLowerCase().replace("desc", "").trim()).descending();
        }

        return null;
    }

    private Sort single(Sort sort, String orderBy) throws RuntimeException {
        String[] orderByClause;

        orderByClause = orderBy.contains(":")
                ? orderBy.split(":")
                : orderBy.split(" ");

        if (orderByClause.length > 1) {
            Sort.Direction direction;

            if (orderByClause[1].equalsIgnoreCase("asc"))
                direction = Sort.Direction.Ascending;
            else if (orderByClause[1].equalsIgnoreCase("desc"))
                direction = Sort.Direction.Descending;
            else throw new RuntimeException("sort is not usable");

            return sort != null
                    ? sort.and(orderByClause[0], direction)
                    : Sort.by(orderByClause[0], direction);

        } else {
            return sort != null
                    ? sort.and(orderBy).descending()
                    : Sort.by(orderBy).ascending();
        }
    }

}
