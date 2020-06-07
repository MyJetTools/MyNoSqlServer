using System;
using System.Collections.Generic;
using MyNoSqlServer.Abstractions;

namespace MyNoSqlServer.Domains.Json
{
    
    public class DynamicEntity : IMyNoSqlDbEntity
    {
        
        public Dictionary<string, IJsonFirstLine> Raw { get; }
        public DynamicEntity(Dictionary<string, IJsonFirstLine> raw)
        {
            Raw = raw;

            if (Raw.ContainsKey(RowJsonUtils.PartitionKeyFieldName))
                PartitionKey = Raw[RowJsonUtils.PartitionKeyFieldName].Value.AsJsonString();
            
            if (Raw.ContainsKey(RowJsonUtils.RowKeyFieldName))
                RowKey = Raw[RowJsonUtils.RowKeyFieldName].Value.AsJsonString();
            
            if (Raw.ContainsKey(RowJsonUtils.TimeStampFieldName))
                TimeStamp = Raw[RowJsonUtils.TimeStampFieldName].Value.AsJsonString();
        }

        public DynamicEntity UpdateTimeStamp(string timeStamp)
        {

            TimeStamp = timeStamp;
            
            var timeStampJsonLine = new MyJsonFirstLevelFieldData(RowJsonUtils.TimeStampFieldName, TimeStamp);
            
            if (Raw.ContainsKey(RowJsonUtils.TimeStampFieldName))
                Raw[RowJsonUtils.TimeStampFieldName] = timeStampJsonLine;
            else
                Raw.Add(RowJsonUtils.TimeStampFieldName, timeStampJsonLine);

            return this;
        }
        
        public string PartitionKey { get; set; }
        public string RowKey { get; set; }
        public string TimeStamp { get; set; }
        public DateTime? Expires { get; set; }
    }
}