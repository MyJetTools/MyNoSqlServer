using System;
using Microsoft.Extensions.DependencyInjection;
using MyNoSqlServer.Domains.DataReadersBroadcast;
using MyNoSqlServer.Domains.Db;
using MyNoSqlServer.Domains.Logs;
using MyNoSqlServer.Domains.Nodes;
using MyNoSqlServer.Domains.Persistence;
using MyNoSqlServer.Domains.SnapshotSaver;
using MyNoSqlServer.Domains.TransactionEvents;

namespace MyNoSqlServer.Domains
{
    public static class ServiceBinder
    {
        public static void BindDomainsServices(this IServiceCollection services)
        {
            services.AddSingleton<DbInstance>();
            
            services.AddSingleton<ISnapshotSaverScheduler, SnapshotSaverScheduler>();
            
            services.AddSingleton<PersistenceHandler>();
            
            services.AddSingleton<GlobalVariables>();
            
            services.AddSingleton<DbOperations>();
            services.AddSingleton<NodesSyncOperations>();

            services.AddSingleton<DataInitializer>();

            services.AddSingleton<SyncEventsDispatcher>();
            services.AddSingleton<NodeSessionsList>();

            services.AddSingleton<AppLogs>();
        }


        public static void LinkDomainServices(this IServiceProvider sp)
        {
            var dispatcher = sp.GetRequiredService<SyncEventsDispatcher>();
            dispatcher.SubscribeOnSyncEvent(sp.GetRequiredService<PersistenceHandler>().PersistEvent);
            dispatcher.SubscribeOnSyncEvent(sp.GetRequiredService<IDataReadersBroadcaster>().BroadcastEvent);
            dispatcher.SubscribeOnSyncEvent(sp.GetRequiredService<NodeSessionsList>().NewEvent);
            
        }
    }
}