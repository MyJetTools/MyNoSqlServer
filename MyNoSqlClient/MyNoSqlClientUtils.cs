using System.Threading.Tasks;
using Flurl;

namespace MyNoSqlClient
{
    public static class MyNoSqlClientUtils
    {

        public static string ToHttpContract(this DataSynchronizationPeriod dataSynchronizationPeriod)
        {
            switch (dataSynchronizationPeriod)
            {
                case DataSynchronizationPeriod.Immediately:
                    return "0";
                
                case DataSynchronizationPeriod.Sec1:
                    return "1";
                
                case DataSynchronizationPeriod.Sec5:
                    return "5";

                case DataSynchronizationPeriod.Sec15:
                    return "15";
                
                case DataSynchronizationPeriod.Sec30:
                    return "30";

                case DataSynchronizationPeriod.Min1:
                    return "60";
                
            }

            return "5";
        }

        public static Url AppendDataSyncPeriod(this Url url, DataSynchronizationPeriod dataSynchronizationPeriod)
        {
            return url.SetQueryParam("syncPeriod", dataSynchronizationPeriod.ToHttpContract());
        }
            
        public static async Task<T> DeleteAsync<T>(
            this IMyNoSqlServerClient<MyNoSqlIndex> indexTableStorage,
            string indexPartitionKey,
            string indexRowKey,
            IMyNoSqlServerClient<T> tableStorage)
            where T : class, IMyNoSqlTableEntity, new()
        {
            var index = await indexTableStorage.DeleteAsync(indexPartitionKey, indexRowKey);
            if (index == null)
                return default;
            return await tableStorage.DeleteAsync(index.PrimaryPartitionKey, index.PrimaryRowKey);
        }
    }
}