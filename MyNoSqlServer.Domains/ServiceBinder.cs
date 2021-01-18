using MyDependencies;
using MyNoSqlServer.Domains.Db;
using MyNoSqlServer.Domains.Db.Operations;
using MyNoSqlServer.Domains.GarbageCollection;
using MyNoSqlServer.Domains.Persistence;
using MyNoSqlServer.Domains.SnapshotSaver;
using MyNoSqlServer.Domains.SnapshotSaver.Implementation;

namespace MyNoSqlServer.Domains
{
    public static class ServiceBinder
    {
        public static void BindDomainsServices(this IServiceRegistrator sr)
        {
            sr.Register<DbInstance>();
            
            sr.Register<SnapshotSaverEngine>();
            
            sr.Register<ISnapshotSaverScheduler, SnapshotSaverScheduler>();
            
            sr.Register<PersistenceHandler>();
            
            sr.Register<GlobalVariables>();
            
            sr.Register<DbTableWriteOperations>();
            sr.Register<DbTableReadOperationsWithExpiration>();
            sr.Register<ExpiredEntitiesGarbageCollector>();
        }
    }
}