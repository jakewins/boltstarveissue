package org.neo4j;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import org.neo4j.collection.RawIterator;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.proc.CallableProcedure;
import org.neo4j.kernel.api.proc.Context;
import org.neo4j.kernel.api.proc.ProcedureSignature;
import org.neo4j.kernel.configuration.BoltConnector;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import static org.neo4j.kernel.api.proc.ProcedureSignature.procedureSignature;
import static org.neo4j.kernel.configuration.BoltConnector.EncryptionLevel.OPTIONAL;

public class BoltStarveIssue
{
    // Run this with `-Dorg.neo4j.selectorThreads=1` to limit IO threads to 1,
    // just to make the issue deterministic; otherwise some connections will succeed
    // as they get assigned to non-blocked IO threads.

    public static void main(String ... argv) throws IOException, InterruptedException, ProcedureException
    {
        // Given we start a Bolt server
        Neo4jWithBolt neo4j = new Neo4jWithBolt();
        neo4j.startDatabase( (config) -> {} );
        addSleepProcedure( neo4j );

        // Given we connect to it with a driver
        Driver driver = GraphDatabase.driver( "bolt://localhost:7687" );

        // And we queue up a lot of slow cypher queries
        runLargeBatchInBackground( driver );

        // .. wait just a second to give the thread above time to get stuck
        Thread.sleep( 1000 );

        // The server is now dead in the water, because the IO thread
        // is stuck in RunnableBoltWorker#enqueue(..); new connections can't
        // be made, existing connections will not have their messages read
        // off the network.
        System.out.println("[MainThread] Making second connection..");
        Session session2 = driver.session();
        session2.run( "RETURN 1" ).single().get(0);
        System.out.println("[MainThread] Got result!");

        // Note that this has nothing to do with actual resource starvation:
        // there is just one query running; all other cores the db has available
        // are idle.
    }

    private static void runLargeBatchInBackground( Driver driver )
    {
        new Thread(() -> {
            System.out.println("[BatchThread] Starting batch of queries..");
            try(Session session = driver.session())
            {
                // What happens here is that the driver will send 1000 queries in one
                // go; but the job queue for a session is just 100 items long, so the
                // queue will get filled. If the driver limited the number of in-flight
                // messages per session to 100, this issue would go away.

                // This could be made dynamic - to allow users to set larger queue sizes -
                // if Bolt implemented ReactiveSockets-style back pressure from the server
                session.writeTransaction( (tx) -> {
                    for ( int i = 0; i < 1000; i++ )
                    {
                        // In the real world, this would be some slow analytical query,
                        // or some expensive/contended insert operation
                        tx.run("CALL boltissue.sleep()");
                    }
                    tx.success();
                    return null;
                });
            }
            finally
            {
                System.out.println("[BatchThread] Done.");
            }
        }).start();
    }

    private static void addSleepProcedure( Neo4jWithBolt neo4j ) throws ProcedureException
    {
        neo4j.db().getDependencyResolver().resolveDependency( Procedures.class ).register(
                new CallableProcedure.BasicProcedure(
                        procedureSignature("boltissue", "sleep")
                                .out( ProcedureSignature.VOID )
                                .build())
        {
            @Override
            public RawIterator<Object[],ProcedureException> apply( Context context, Object[] objects ) throws ProcedureException
            {
                try
                {
                    Thread.sleep( 60 * 1000 );
                }
                catch ( InterruptedException e )
                {
                    throw new ProcedureException( Status.General.UnknownError, e, "Interrupted" );
                }
                return RawIterator.empty();
            }
        } );
    }

}

class Neo4jWithBolt
{
    private final Consumer<Map<String,String>> configure;
    private GraphDatabaseFactory graphDatabaseFactory;
    private GraphDatabaseService gdb;
    private File workingDirectory;

    Neo4jWithBolt()
    {
        this.graphDatabaseFactory = new GraphDatabaseFactory();
        this.configure = settings ->
        {
        };
        this.workingDirectory = new File("./testdata");
    }

    void startDatabase( Consumer<Map<String,String>> overrideSettingsFunction ) throws IOException
    {
        if ( gdb != null )
        {
            return;
        }

        Map<String,String> settings = configure( overrideSettingsFunction );
        File storeDir = new File( workingDirectory, "storeDir" );
        gdb = graphDatabaseFactory.newEmbeddedDatabaseBuilder( storeDir ).
                setConfig( settings ).newGraphDatabase();
    }

    private Map<String,String> configure( Consumer<Map<String,String>> overrideSettingsFunction ) throws IOException
    {
        Map<String,String> settings = new HashMap<>();
        settings.put( new BoltConnector( "bolt" ).type.name(), "BOLT" );
        settings.put( new BoltConnector( "bolt" ).enabled.name(), "true" );
        settings.put( new BoltConnector( "bolt" ).encryption_level.name(), OPTIONAL.name() );
        configure.accept( settings );
        overrideSettingsFunction.accept( settings );
        return settings;
    }

    GraphDatabaseAPI db()
    {
        return (GraphDatabaseAPI) gdb;
    }
}