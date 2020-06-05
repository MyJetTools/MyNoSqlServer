using System;
using System.Linq;
using System.Text;
using MyNoSqlServer.Common;
using MyNoSqlServer.Domains;
using MyNoSqlServer.Domains.Db.Rows;

namespace MyNoSqlServerUnitTests
{
    public static class TestDecorators
    {

        public static byte[] AsJsonByteArray(this IMyNoSqlDbEntity myNoSqlDbEntity)
        {
            return Encoding.UTF8.GetBytes(Newtonsoft.Json.JsonConvert.SerializeObject(myNoSqlDbEntity));
        }


        /*
        public static T DeserializeDbEntity<T>(this byte[] bytes)
        {
            var str = Encoding.UTF8.GetString(bytes);
            return Newtonsoft.Json.JsonConvert.DeserializeObject<T>(str);
        }
        */


        public static string GetValue(this DbRow dbRow, string fieldName)
        {
            var fields = dbRow.Data.AsMyMemory().ParseFirstLevelOfJson();
            var property = fields.FirstOrDefault(itm => itm.Field.AsJsonString() == fieldName);
            
            if (property == null)
                throw new Exception($"Property with name {fieldName} is not found");

            return property.Value.AsJsonString();
        }
    }
}