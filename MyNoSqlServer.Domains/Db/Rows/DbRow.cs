using System;
using System.Collections.Generic;
using MyNoSqlServer.Domains.Json;
using MyNoSqlServer.Domains.Query;

namespace MyNoSqlServer.Domains.Db.Rows
{
    public class DbRow
    {
        private DbRow(string partitionKey, string rowKey, string timestamp, DateTime? expires, byte[] data)
        {
            if (string.IsNullOrEmpty(partitionKey))
                throw new Exception("Partition key can not be empty");
            
            if (string.IsNullOrEmpty(rowKey))
                throw new Exception("Row key can not be empty");

            PartitionKey = partitionKey;
            RowKey = rowKey;
            TimeStamp = timestamp;
            Data = data;
            Expires = expires;
        }
        
        public string PartitionKey { get; }
        public string RowKey { get; }
        public string TimeStamp { get; }
        public byte[] Data { get; }
        
        public DateTime? Expires { get; internal set; }

        public static DbRow CreateNew(DynamicEntity entity, DateTime now)
        {
            var timeStamp = now.ToTimeStampString();
            entity.UpdateTimeStamp(timeStamp);
            return new DbRow(entity.PartitionKey, entity.RowKey, timeStamp, entity.Expires, entity.AsDbRowJson());
        }
        
        public static DbRow Restore(DynamicEntity entity)
        {
            return new DbRow(entity.PartitionKey, entity.RowKey, entity.TimeStamp, entity.Expires, entity.AsDbRowJson());
        }

        public bool MatchesQuery(IDictionary<string, List<QueryCondition>> conditionsDict)
        {
            throw new NotImplementedException("Temporary disabled the ability to filter within Fields of DbRow");
        }

    }
    
}