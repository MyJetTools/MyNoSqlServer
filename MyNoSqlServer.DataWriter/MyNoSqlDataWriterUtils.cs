using Flurl;
using MyNoSqlServer.Abstractions;

namespace MyNoSqlServer.DataWriter
{
    public static class MyNoSqlDataWriterUtils
    {
        public static Url AppendDataSyncPeriod(this Url url, DataSynchronizationPeriod dataSynchronizationPeriod)
        {
            return url.SetQueryParam("syncPeriod", dataSynchronizationPeriod.AsString(null));
        }

        public static Url WithPartitionKeyAsQueryParam(this Url url, string partitionKey)
        {
            return url.SetQueryParam("partitionKey", partitionKey);
        }
        
        public static Url WithRowKeyAsQueryParam(this Url url, string rowKey)
        {
            return url.SetQueryParam("rowKey", rowKey);
        }
        
        public static Url WithTableNameAsQueryParam(this Url url, string tableName)
        {
            return url.SetQueryParam("rowKey", tableName);
        }

    }
}