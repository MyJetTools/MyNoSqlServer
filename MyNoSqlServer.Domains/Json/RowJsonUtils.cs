using System;
using System.Collections.Generic;
using MyNoSqlServer.Domains.Db.Rows;

namespace MyNoSqlServer.Domains.Json
{
    public static class RowJsonUtils
    {

        public const string PartitionKeyFieldName = "PartitionKey";
        public const string RowKeyFieldName = "RowKey";
        public const string TimeStampFieldName = "TimeStamp";
        public const string ExpiresFieldName = "Expires";
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

        public static bool IsNull(this in ReadOnlyMemory<byte> span)
        {
            return IsMyFields(span, "null", "NULL");
        }


        internal static ReadOnlyMemory<byte> ToJsonReadOnlyMemory(this string src)
        {
            var result = new byte[src.Length + 2];

            result[0] = JsonByteArrayReader.DoubleQuote;

            var i = 1;
            foreach (var c in src)
            {
                result[i] = (byte)c;
                i++;
            }
            result[i] = JsonByteArrayReader.DoubleQuote;
            return result;
        }


        public static DynamicEntity MergeEntities(this DbRow dbRow, DynamicEntity fieldsToMerge)
        {
            
            var dbRowEntity = dbRow.Data.ParseDynamicEntity();
            
            var fieldsInDb =new Dictionary<string, IJsonFirstLine>(dbRowEntity.Raw);
            
            foreach (var (fieldName, value) in fieldsToMerge.Raw)
            {
                if (fieldsInDb.ContainsKey(fieldName))
                    fieldsInDb[fieldName] = value;
                else
                    fieldsInDb.Add(fieldName,value);
            }

            return new DynamicEntity(fieldsInDb);

        }

    }
}