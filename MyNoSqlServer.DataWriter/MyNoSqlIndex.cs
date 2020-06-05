using MyNoSqlServer.Abstractions;

namespace MyNoSqlServer.DataWriter
{
    public class MyNoSqlIndex : MyNoSqlEntity
    {
        public MyNoSqlIndex()
        {
            
        }
        
        public MyNoSqlIndex(
            string partitionKey,
            string rowKey,
            string primaryPartitionKey,
            string primaryRowKey)
        {
            PartitionKey = partitionKey;
            RowKey = rowKey;
            PrimaryPartitionKey = primaryPartitionKey;
            PrimaryRowKey = primaryRowKey;
        }

        public MyNoSqlIndex(string partitionKey, string rowKey, IMyNoSqlEntity tableEntity)
        {
            PartitionKey = partitionKey;
            RowKey = rowKey;
            PrimaryPartitionKey = tableEntity.PartitionKey;
            PrimaryRowKey = tableEntity.RowKey;
        }

        public string PrimaryPartitionKey { get; set; }

        public string PrimaryRowKey { get; set; }

        public static MyNoSqlIndex Create(
            string partitionKey,
            string rowKey,
            IMyNoSqlEntity tableEntity)
        {
            var azureIndex = new MyNoSqlIndex
            {
                PartitionKey = partitionKey,
                RowKey = rowKey,
                PrimaryPartitionKey = tableEntity.PartitionKey,
                PrimaryRowKey = tableEntity.RowKey
            };
            return azureIndex;
        }

        public static MyNoSqlIndex Create(
            string partitionKey,
            string rowKey,
            string primaryPartitionKey,
            string primaryRowKey)
        {
            var azureIndex = new MyNoSqlIndex
            {
                PartitionKey = partitionKey,
                RowKey = rowKey,
                PrimaryPartitionKey = primaryPartitionKey,
                PrimaryRowKey = primaryRowKey
            };
            return azureIndex;
        }
    }
}