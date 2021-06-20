using Microsoft.Extensions.DependencyInjection;
using MyNoSqlServer.AzureStorage.TablesStorage;
using MyNoSqlServer.Domains.Persistence;

namespace MyNoSqlServer.AzureStorage
{
    public static class AzureStorageBinder
    {
        public static void BindAzureStorage(this IServiceCollection services, string connectionString)
        {
            if (string.IsNullOrEmpty(connectionString))
                return;
            
            services.AddSingleton<ITablePersistenceStorage>(new AzureTablePersistenceStorage(connectionString));

        }
    }
    
}