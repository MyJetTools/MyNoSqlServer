using System;
using System.Text;
using MyNoSqlServer.Abstractions;

namespace MyNoSqlServer.Grpc
{
    public static class EntitiesContentTypesSupport
    {
        public static T DeserializeEntity<T>(this TableEntityTransportGrpcContract contract)  where T : IMyNoSqlDbEntity, new()
        {
            if (contract.ContentType == GrpcContentType.Json)
            {
                var jsonEntity = Encoding.UTF8.GetString(contract.Content);
                return Newtonsoft.Json.JsonConvert.DeserializeObject<T>(jsonEntity);
            }

            throw new NotSupportedException("Not supported content type:" + contract.ContentType);
        }

        public static TableEntityTransportGrpcContract SerializeEntity(this IMyNoSqlDbEntity myNoSqlDbEntity)
        {
            var json = Newtonsoft.Json.JsonConvert.SerializeObject(myNoSqlDbEntity);

            return new TableEntityTransportGrpcContract
            {
                ContentType = GrpcContentType.Json,
                Content = Encoding.UTF8.GetBytes(json)
            };
        }
        
    }
}