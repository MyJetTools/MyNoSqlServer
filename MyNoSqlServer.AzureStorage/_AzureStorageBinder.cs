using System;
using Microsoft.Extensions.DependencyInjection;
using MyNoSqlServer.AzureStorage.TablesStorage;
using MyNoSqlServer.Domains.Logs;
using MyNoSqlServer.Domains.Persistence;
using MyNoSqlServer.Domains.Persistence.Blobs;

namespace MyNoSqlServer.AzureStorage
{
    public static class AzureStorageBinder
    {
        public static void BindAzureStorage(this IServiceCollection services, string connectionString)
        {

            var storage = new AzureTablePersistenceStorage(connectionString);
            services.AddSingleton<IBlobPersistenceStorage>(storage);
            services.AddSingleton<ITablesPersistenceReader>(storage);
            
            services.AddSingleton<IPersistenceShutdown, BlobsSaver>();
        }


        public static void Init(IServiceProvider sp)
        {
            AzureStorageBlobDecorators.Init(sp);
            
            var persistence = (AzureTablePersistenceStorage)sp.GetRequiredService<IBlobPersistenceStorage>();
            persistence.Inject(sp.GetRequiredService<AppLogs>());
        }
    }
    
}