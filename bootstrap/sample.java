////////////////////////////////////////////////////////////////////////////////
//
// Simple program that writes to Cassandra using the Astyanax client
// framework.
//

import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;

import org.apache.commons.lang.time.StopWatch;

public class sample {

    public static void main(String[] args) throws Exception{
        AstyanaxContext<Keyspace> context = new AstyanaxContext.Builder()
        .forCluster("Test Cluster")
        .forKeyspace("MyKeyspace")
        .withAstyanaxConfiguration(new AstyanaxConfigurationImpl()
                .setDiscoveryType(NodeDiscoveryType.NONE)
        )
        .withConnectionPoolConfiguration(new ConnectionPoolConfigurationImpl("MyConnectionPool")
                .setPort(9160)
                .setMaxConnsPerHost(20)
                .setSeeds("127.0.0.1:9160")
        )
        .withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
        .buildKeyspace(ThriftFamilyFactory.getInstance());

        context.start();
        Keyspace keyspace = context.getEntity();

        ColumnFamily<String, String> CF_USER_INFO =
                new ColumnFamily<String, String>(
                "User",                 // Column Family Name
                StringSerializer.get(),   // Key Serializer
                StringSerializer.get());  // Column Serializer

        // Inserting data
        MutationBatch m = keyspace.prepareMutationBatch();

        int NUM_EVENTS = 100000;
        int BATCH_SIZE = 2000;

        StopWatch clock = new StopWatch();
        clock.start();
        for (int i = 0; i < NUM_EVENTS; ++i) {
                m.withRow(CF_USER_INFO, "2012-Jul-16-1900")
                .putColumn("" + i,  "This is user named " + i, null);
                if (i % BATCH_SIZE == 0) {
                        OperationResult<Void> result = m.execute();
                }
        }
        clock.stop();

        System.out.println( "It took " + clock.getTime() + " milliseconds" );
        System.out.println( "" + (NUM_EVENTS*1000)/clock.getTime() + " events per second." );
    }

}
