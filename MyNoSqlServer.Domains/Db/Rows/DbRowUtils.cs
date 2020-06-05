using System.Collections.Generic;
using System.Text;
using MyNoSqlServer.Common;

namespace MyNoSqlServer.Domains.Db.Rows
{
    
    public static class DbRowUtils
    {
        public static ChunkedStream ToJsonArray(this IEnumerable<DbRow> dbRows)
        {
            var result = new ChunkedStream();
            result.Write(OpenArray);

            var firstLine = true;
            foreach (var row in dbRows)
            {
                if (firstLine)
                    firstLine = false;
                else
                    result.Write(Comma);

                result.Write(row.Data);
            }

            result.Write(CloseArray);

            return result;

        }



        private static readonly byte[] OpenArray = {JsonByteArrayReader.OpenArray};
        private static readonly byte[] CloseArray = {JsonByteArrayReader.CloseArray};
        
        private static readonly byte[] OpenBracket = {JsonByteArrayReader.OpenBracket};
        private static readonly byte[] CloseBracket = {JsonByteArrayReader.CloseBracket};
        
        private static readonly byte[] DoubleColumn = {JsonByteArrayReader.DoubleColumn};
        private static readonly byte[] Comma = {JsonByteArrayReader.Comma};

        private static readonly byte[] TimestampFieldAndDoubleColumn = Encoding.UTF8.GetBytes(JsonUtils.TimeStampField+":");
      

        public static List<MyJsonFirstLevelFieldData> InjectTimeStamp(
            this List<MyJsonFirstLevelFieldData> src, string timeStamp)
        {

            var timeStampAsBytes = new byte[timeStamp.Length+2];
            timeStampAsBytes[0] = JsonByteArrayReader.DoubleQuote;
            timeStampAsBytes[^1] = JsonByteArrayReader.DoubleQuote;

            var i = 1;
            foreach (var c in timeStamp)
                timeStampAsBytes[i++] = (byte) c;
            
            var index = src.FindIndex(itm => itm.Field.IsTimeStampField());

            var timeStampField = new MyJsonFirstLevelFieldData(
                JsonUtils.TimeSpanKeyAsArray,
                timeStampAsBytes);
            
            if (index < 0)
                src.Add(timeStampField);
            else
                src[index] = timeStampField;

            return src;
        }

        public static IMyNoSqlDbEntity GetEntityInfo(this IEnumerable<MyJsonFirstLevelFieldData> fields)
        {
            var result = new MyNoSqlDbEntity();

            foreach (var itm in fields)
            {
                if (itm.Field.IsPartitionKey())
                    result.PartitionKey = itm.Value.AsJsonString();
                
                if (itm.Field.IsRowKey())
                    result.RowKey = itm.Value.AsJsonString();
                
                if (itm.Field.IsTimeStampField())
                    result.Timestamp = itm.Value.AsJsonString();


                if (result.PartitionKey != null && result.RowKey != null && result.Timestamp != null)
                    break;
            }

            return result;
        }
        
        
    }
    
}