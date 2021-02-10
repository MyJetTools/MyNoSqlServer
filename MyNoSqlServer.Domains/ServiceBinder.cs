using MyDependencies;
using MyNoSqlServer.Domains.Db;
using MyNoSqlServer.Domains.Persistence;
using MyNoSqlServer.Domains.SnapshotSaver;

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
            
            sr.Register<DbOperations>();
        }
    }
}