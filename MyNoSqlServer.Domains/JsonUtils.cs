using System;
using System.Text;

namespace MyNoSqlServer.Domains
{
    public static class JsonUtils
    {
        public const string PartitionKeyUpperKeyField = "\"PARTITIONKEY\"";
        public const string PartitionKeyLowKeyField = "\"partitionkey\"";

        
        
        public const string RowKeyUpperKeyField = "\"ROWKEY\"";
        public const string RowKeyLowKeyField = "\"rowkey\"";
        

        public const string TimeStampUpperKeyField = "\"TIMESTAMP\"";
        public const string TimeStampLowKeyField = "\"timestamp\"";
        
        public const string TimeStampField = "\"TimeStamp\"";
        
        public static byte[] TimeSpanKeyAsArray => Encoding.UTF8.GetBytes(TimeStampField);
        
        public static string ToTimeStampString(this DateTime timeStamp)
        {
            return timeStamp.ToString("O");
        }
        
        public static bool IsMyFields(in ReadOnlyMemory<byte> span, string lowerCase, string upperCase)
        {
            if (span.Length != lowerCase.Length)
                return false;

            var i = 0;

            foreach (var b in span.Span)
            {
                if (b != lowerCase[i] && b != upperCase[i])
                    return false;
                i++;
            }

            return true;
        }
        
        public static bool IsTimeStampField(this in ReadOnlyMemory<byte> span)
        {
            return IsMyFields(span, TimeStampLowKeyField, TimeStampUpperKeyField);
        }
        
        public static bool IsPartitionKey(this in ReadOnlyMemory<byte> span)
        {
            return IsMyFields(span, PartitionKeyLowKeyField, PartitionKeyUpperKeyField);
        }
        
        public static bool IsRowKey(this in ReadOnlyMemory<byte> span)
        {
            return IsMyFields(span, RowKeyLowKeyField, RowKeyUpperKeyField);
        }
        
        public static bool IsNull(this in ReadOnlyMemory<byte> span)
        {
            return IsMyFields(span, "null", "NULL");
        } 

    }
}