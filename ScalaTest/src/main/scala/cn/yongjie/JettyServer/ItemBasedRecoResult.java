package cn.yongjie.JettyServer;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;


@Path("/ws/v1/recom")
public class ItemBasedRecoResult {


    @GET
    @Path("/{userid}")
    @Produces(MediaType.APPLICATION_JSON)
    public RecommendedItems getRecoItems(@PathParam("userid") String userid) {
        RecommendedItems recommendedItems = new RecommendedItems();
        Long[] a = new Long[1];
        a[0] = 12345L;
        recommendedItems.setItems(a);


        return recommendedItems;
    }
}
