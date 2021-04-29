using System;
using System.Text;
using MyNoSqlServer.Abstractions;

namespace MyNoSqlServer.Grpc
{
    public static class EntitiesContentTypesSupport
    {
        public static T DeserializeEntity<T>(this TableEntityTransportGrpcContract contract)  where T : IMyNoSqlDbEntity, new()
        {
            if (contract.ContentType == ContentType.Json)
            {
                var jsonEntity = Encoding.UTF8.GetString(contract.Content);
                return Newtonsoft.Json.JsonConvert.DeserializeObject<T>(jsonEntity);
            }

            throw new NotSupportedException("Not supported content type:" + contract.ContentType);
        }
        
    }
}