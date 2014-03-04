package example;
import com.sun.net.httpserver.HttpServer;
import com.sun.jersey.api.container.httpserver.HttpServerFactory;
import java.io.IOException;

import javax.ws.rs.GET;
import javax.ws.rs.Produces;
import javax.ws.rs.Path;

/**
 * User: alexsilva Date: 3/4/14 Time: 10:34 AM
 */

@Path("/id")
public class IdGeneratorService {

    @GET
    @Produces("text/plain")
    public String getId() {
	    return WMRedisClient.INSTANCE.getId(WMRedisClient.USER_ID_SET) + "\n";
    }

    public static void main(String[] args) throws IOException {
        HttpServer server = HttpServerFactory.create("http://localhost:9998/");
        server.start();

        System.out.println("Server running");
        System.out.println("Visit: http://localhost:9998/id");
        System.out.println("Hit return to stop...");
        System.in.read();
        System.out.println("Stopping server");
        server.stop(0);
        System.out.println("Server stopped");
    }
}