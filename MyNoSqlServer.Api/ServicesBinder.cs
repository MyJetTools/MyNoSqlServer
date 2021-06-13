using Grpc.Net.Client;
using Microsoft.Extensions.DependencyInjection;
using MyNoSqlServer.Api.DataReadersTcpServer;
using MyNoSqlServer.AzureStorage;
using MyNoSqlServer.Domains.DataReadersBroadcast;
using MyNoSqlServer.Domains.Persistence;
using MyNoSqlServer.NodePersistence;
using ProtoBuf.Grpc.Client;

namespace MyNoSqlServer.Api
{
    public static class ServicesBinder
    {

        public static void BindAsNodeServices(this IServiceCollection sc, SettingsModel settings)
        {
            sc.AddSingleton<IMyNoSqlNodePersistenceSettings>(settings);
            
            GrpcClientFactory.AllowUnencryptedHttp2 = true;
            
            sc.AddSingleton(GrpcChannel
                .ForAddress(settings.PersistenceDest)
                .CreateGrpcService<ISnapshotStorage>());
        }

        public static void BindAsRootNodeServices(this IServiceCollection sc, SettingsModel settings)
        {
            sc.BindAzureStorage(settings.PersistenceDest);
        }
        
        public static void BindDataReadersTcpServices(this IServiceCollection services)
        {
            services.AddSingleton<IDataReadersBroadcaster, DataReadersTcpBroadcaster>();
            services.AddSingleton<DataReaderSubscribers>();
        }
        
        
    }
}