using MyNoSqlServer.Domains;
using MyNoSqlServer.Domains.SnapshotSaver;

namespace MyNoSqlServer.AzureStorage
{
    public static class AzureStorageBinder
    {
        public static void BindAzureStorage(this string connectionString)
        {
            if (string.IsNullOrEmpty(connectionString))
                return;

            ServiceLocator.SnapshotStorage = new AzureBlobSnapshotStorage(connectionString);
        }
    }
    
}