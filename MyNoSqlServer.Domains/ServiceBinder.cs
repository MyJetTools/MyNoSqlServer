using Microsoft.Extensions.DependencyInjection;
using MyNoSqlServer.Domains.Db;
using MyNoSqlServer.Domains.Persistence;
using MyNoSqlServer.Domains.SnapshotSaver;

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
        }
    }
}