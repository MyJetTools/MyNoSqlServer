using System;
using System.Text;
using MyNoSqlServer.Abstractions;
using MyNoSqlServer.Common;
using MyNoSqlServer.Domains.Db.Rows;
using MyNoSqlServer.Domains.Json;

namespace MyNoSqlServer.Tests
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
            var fields = dbRow.Data.AsMyMemory().ParseDynamicEntity();
            var property = fields.Raw[fieldName];
            
            if (property == null)
                throw new Exception($"Property with name {fieldName} is not found");

            return property.Value.AsJsonString();
        }
    }
}