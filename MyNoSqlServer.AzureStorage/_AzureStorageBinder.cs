using System;
using Microsoft.Extensions.DependencyInjection;
using MyNoSqlServer.AzureStorage.TablesStorage;
using MyNoSqlServer.Domains.Logs;
using MyNoSqlServer.Domains.Persistence;

namespace MyNoSqlServer.AzureStorage
{
    public static class AzureStorageBinder
    {
        public static void BindAzureStorage(this IServiceCollection services, string connectionString)
        {
            services.AddSingleton<ITablePersistenceStorage>(new AzureTablePersistenceStorage(connectionString));
        }


        public static void Init(IServiceProvider sp)
        {
            AzureStorageBlobDecorators.Init(sp);
            
            var persistence = (AzureTablePersistenceStorage)sp.GetRequiredService<ITablePersistenceStorage>();
            persistence.Inject(sp.GetRequiredService<AppLogs>());
        }
    }
    
}