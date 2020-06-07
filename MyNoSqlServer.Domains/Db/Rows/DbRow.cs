using System;
using System.Collections.Generic;
using System.Linq;
using MyNoSqlServer.Abstractions;
using MyNoSqlServer.Common;
using MyNoSqlServer.Domains.Json;
using MyNoSqlServer.Domains.Query;

namespace MyNoSqlServer.Domains.Db.Rows
{
    public class DbRow
    {
        private DbRow(string partitionKey, string rowKey, string timestamp, byte[] data)
        {
            if (string.IsNullOrEmpty(partitionKey))
                throw new Exception("Partition key can not be empty");
            
            if (string.IsNullOrEmpty(rowKey))
                throw new Exception("Row key can not be empty");

            PartitionKey = partitionKey;
            RowKey = rowKey;
            TimeStamp = timestamp;
            Data = data;
        }
        
        public string PartitionKey { get; }

        public string RowKey { get; }
        public string TimeStamp { get; private set; }
        public byte[] Data { get; private set; }


        

        public static DbRow RestoreSnapshot(IMyNoSqlDbEntity techData, IMyMemory data)
        {
            return new DbRow(techData.PartitionKey, techData.RowKey, techData.TimeStamp, data.AsArray());
        }
        
        public static DbRow CreateNew(DynamicEntity entity, DateTime now)
        {
            var timeStamp = now.ToTimeStampString();
            entity.UpdateTimeStamp(timeStamp);
            return new DbRow(entity.PartitionKey, entity.RowKey, timeStamp, entity.AsDbRowJson());
        }
        
        public void Replace(DynamicEntity entity, DateTime now)
        {   TimeStamp = now.ToTimeStampString();
            entity.UpdateTimeStamp(TimeStamp);
            Data = entity.AsDbRowJson();
        }

        public bool MatchesQuery(IDictionary<string, List<QueryCondition>> conditionsDict)
        {
            throw new NotImplementedException("Temporary disabled the ability to filter within Fields of DbRow");
        }

    }

    public static class DbRowHelpers
    {
        public static DbRow ToDbRow(this IMyMemory myMemory)
        {
            var entity = myMemory.ParseDynamicEntity();
            return DbRow.CreateNew(entity, DateTime.UtcNow);
        }
    }
    
}