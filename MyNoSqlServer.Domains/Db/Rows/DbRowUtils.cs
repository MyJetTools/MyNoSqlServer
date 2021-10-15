using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using MyNoSqlServer.Common;
using MyNoSqlServer.Domains.Json;

namespace MyNoSqlServer.Domains.Db.Rows
{
    
    public static class DbRowUtils
    {


        public static DateTime ParseEntityDateTime(this DynamicEntity dynamicEntity)
        {

            try
            {
                return dynamicEntity.TimeStamp == null 
                    ? DateTime.UtcNow 
                    : DateTime.Parse(dynamicEntity.TimeStamp);

            }
            catch
            {
                return DateTime.UtcNow;
            }
            
        }
        
        

        public static IReadOnlyList<DbRow> ParseDbRowList(this byte[] src)
        {
            return src.AsMyMemory().SplitJsonArrayToObjects().Select(itm =>
            {
                var dynEntity = itm.ParseDynamicEntity();
                var dt = dynEntity.ParseEntityDateTime();
                return DbRow.CreateNew(itm.ParseDynamicEntity(), dt);
            }).AsReadOnlyList();
        }


        private static void PopulateArrayOfElements(this ChunkedStream stream, IEnumerable<DbRow> dbRows)
        {
            stream.Write(OpenArray);

            var firstLine = true;
            foreach (var row in dbRows)
            {
                if (firstLine)
                    firstLine = false;
                else
                    stream.Write(Comma);

                stream.Write(row.Data);
            }

            stream.Write(CloseArray);
        }
        
        public static ChunkedStream ToJsonArray(this IEnumerable<DbRow> dbRows)
        {
            var result = new ChunkedStream();
         
            result.PopulateArrayOfElements(dbRows);

            return result;
        }
        
        public static ChunkedStream ToJsonArray(this IReadOnlyDictionary<string, IReadOnlyList<DbRow>> dbRows)
        {
            var result = new ChunkedStream();
         
            result.PopulateArrayOfElements(dbRows.SelectMany(itm => itm.Value));

            return result;
        }


        
        public static ChunkedStream ToJsonDictionary(this IReadOnlyDictionary<string, IReadOnlyList<DbRow>> partitions)
        {
            var result = new ChunkedStream();
            result.Write(OpenBracket);

            var partitionFirstLine = true;

            foreach (var (partitionKey, dbRows) in partitions)
            {

                if (partitionFirstLine)
                    partitionFirstLine = false;
                else
                    result.Write(Comma);
                
                result.Write(DoubleQuote);
                result.Write(Encoding.UTF8.GetBytes(partitionKey));
                result.Write(DoubleQuote);
                result.Write(DoubleColumn);
                
                result.PopulateArrayOfElements(dbRows);
            }
       
            
            
            result.Write(CloseBracket);

            return result;
        }



        private static readonly byte[] OpenArray = {JsonByteArrayReader.OpenArray};
        private static readonly byte[] CloseArray = {JsonByteArrayReader.CloseArray};
        
        private static readonly byte[] OpenBracket = {JsonByteArrayReader.OpenBracket};
        private static readonly byte[] CloseBracket = {JsonByteArrayReader.CloseBracket};
        
        private static readonly byte[] DoubleColumn = {JsonByteArrayReader.DoubleColumn};
        private static readonly byte[] Comma = {JsonByteArrayReader.Comma};
        
        private static readonly byte[] DoubleQuote = {JsonByteArrayReader.DoubleQuote};

      //  private static readonly byte[] TimestampFieldAndDoubleColumn = Encoding.UTF8.GetBytes(RowJsonUtils.TimeStampField+":");
        
    }
    
}