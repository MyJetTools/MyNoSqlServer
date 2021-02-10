using System.Collections.Generic;
using MyNoSqlServer.Common;
using MyNoSqlServer.Domains.Json;

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
        
       // private static readonly byte[] OpenBracket = {JsonByteArrayReader.OpenBracket};
       // private static readonly byte[] CloseBracket = {JsonByteArrayReader.CloseBracket};
        
      //  private static readonly byte[] DoubleColumn = {JsonByteArrayReader.DoubleColumn};
        private static readonly byte[] Comma = {JsonByteArrayReader.Comma};

      //  private static readonly byte[] TimestampFieldAndDoubleColumn = Encoding.UTF8.GetBytes(RowJsonUtils.TimeStampField+":");
        
    }
    
}