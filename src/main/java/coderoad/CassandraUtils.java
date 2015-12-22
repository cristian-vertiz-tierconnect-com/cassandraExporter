package coderoad;

import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.ConstantReconnectionPolicy;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.DowngradingConsistencyRetryPolicy;
import com.datastax.driver.core.policies.WhiteListPolicy;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;

public class CassandraUtils
{

    private static Session riotSession = null;
    private static Cluster cluster = null;

    public static Session getSession()
    {
        return riotSession;
    }

    public static void init( String hostin, String keyspace )
    {

        PoolingOptions po = new PoolingOptions();

        po.setMaxConnectionsPerHost(HostDistance.LOCAL, 100);
        po.setCoreConnectionsPerHost(HostDistance.LOCAL, 3);
        po.setMinSimultaneousRequestsPerConnectionThreshold(HostDistance.LOCAL, 0);
        po.setMaxSimultaneousRequestsPerConnectionThreshold(HostDistance.LOCAL, 1);


        Collection<InetSocketAddress> whiteList = new ArrayList<InetSocketAddress>();
        whiteList.add(new InetSocketAddress("127.0.0.1", 9042));
        WhiteListPolicy policy = new WhiteListPolicy(new DCAwareRoundRobinPolicy("datacenter1"), whiteList);

        cluster = Cluster.builder().withPoolingOptions(po)
                .withLoadBalancingPolicy(policy)
                /*.withRetryPolicy(DowngradingConsistencyRetryPolicy.INSTANCE)*/
                /*.withReconnectionPolicy(new ConstantReconnectionPolicy(100L))*/
                .addContactPoint( hostin ).build();
        //cluster.getConfiguration().getSocketOptions().setConnectTimeoutMillis(10000);
        //cluster.getConfiguration().getPoolingOptions().setMaxConnectionsPerHost(HostDistance.LOCAL,100);



        riotSession = cluster.connect( keyspace );


//        cluster = Cluster.builder().addContactPoint( hostin ).build();
//
//		Metadata metadata = cluster.getMetadata();
//		System.out.println( "Connected to cluster: " + metadata.getClusterName() );
//
//		for( Host host : metadata.getAllHosts() )
//		{
//			System.out.println( String.format( "Datacenter: %s; Host: %s; Rack: %s\n", host.getDatacenter(), host.getAddress(), host.getRack() ) );
//		}
//		riotSession = cluster.connect( keyspace );
    }

    public static void shutdown()
    {
        if (cluster != null)
        {
            cluster.close();
        }
    }
}
