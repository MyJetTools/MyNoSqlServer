using System;
using System.Collections.Generic;
using System.Linq;
using MyNoSqlServer.Common;
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
            Timestamp = timestamp;
            Data = data;
        }
        
        public string PartitionKey { get; }

        public string RowKey { get; }
        public string Timestamp { get; }
        public byte[] Data { get; }

        public static DbRow CreateNew(IMyNoSqlDbEntity entity, List<MyJsonFirstLevelFieldData> fields)
        {
            var timeStamp = DateTime.UtcNow.ToTimeStampString();
            fields.InjectTimeStamp(timeStamp);
            return new DbRow(entity.PartitionKey, entity.RowKey, timeStamp, fields.AsDbRowJson());
        }

        public static DbRow RestoreSnapshot(IMyNoSqlDbEntity techData, IMyMemory data)
        {
            return new DbRow(techData.PartitionKey, techData.RowKey, techData.Timestamp, data.AsArray());
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
            var fields = myMemory.ParseFirstLevelOfJson().ToList();
            var entityInfo = fields.GetEntityInfo();
            return DbRow.CreateNew(entityInfo, fields);
        }
    }
    
}