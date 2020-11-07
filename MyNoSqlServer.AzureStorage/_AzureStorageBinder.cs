using MyDependencies;
using MyNoSqlServer.Domains.Persistence;

namespace MyNoSqlServer.AzureStorage
{
    public static class AzureStorageBinder
    {
        public static void BindAzureStorage(this IServiceRegistrator sr,  string connectionString)
        {
            if (string.IsNullOrEmpty(connectionString))
                return;
            
            sr.Register<ISnapshotStorage>(new AzureBlobSnapshotStorage(connectionString));

        }
    }
    
}