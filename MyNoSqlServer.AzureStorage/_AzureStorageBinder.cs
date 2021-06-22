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

            var blobPersistenceStorage = sp.GetService<IBlobPersistenceStorage>();
            var appLogs = sp.GetRequiredService<AppLogs>();

            if (blobPersistenceStorage == null)
            {
                appLogs.WriteInfo(null, "AzureStorageBinder.Init", null, "Instance works and node. Skipping Blob services initialization");
                return;
            }
            
            AzureStorageBlobDecorators.Init(sp);
            
            var persistence = (AzureTablePersistenceStorage)blobPersistenceStorage;
            persistence.Inject(sp.GetRequiredService<AppLogs>());
        }
    }
    
}