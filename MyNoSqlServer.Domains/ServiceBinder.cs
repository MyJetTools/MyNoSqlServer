using System;
using Microsoft.Extensions.DependencyInjection;
using MyNoSqlServer.Domains.DataReadersBroadcast;
using MyNoSqlServer.Domains.Db;
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
            
            services.AddSingleton<SnapshotSaverEngine>();
            
            services.AddSingleton<ISnapshotSaverScheduler, SnapshotSaverScheduler>();
            
            services.AddSingleton<PersistenceHandler>();
            
            services.AddSingleton<GlobalVariables>();
            
            services.AddSingleton<DbOperations>();

            services.AddSingleton<MyNoSqlLogger>();

            services.AddSingleton<SyncEventsDispatcher>();
            services.AddSingleton<PersistenceScheduler>();
        }


        public static void LinkDomainServices(this IServiceProvider sp)
        {
            var dispatcher = sp.GetRequiredService<SyncEventsDispatcher>();
            dispatcher.SubscribeOnSyncEvent(sp.GetRequiredService<PersistenceScheduler>().PublishPersistenceEvent);
            dispatcher.SubscribeOnSyncEvent(sp.GetRequiredService<IDataReadersBroadcaster>().BroadcastEvent);
            
        }
    }
}