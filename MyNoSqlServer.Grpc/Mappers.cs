using System;
using MyNoSqlServer.Abstractions;

namespace MyNoSqlServer.Grpc
{
    public static class Mappers
    {

        public static DbEntityType ToDbEntityContentType(this GrpcContentType contentType)
        {

            switch (contentType)
            {
                case GrpcContentType.Json:
                return DbEntityType.Json;
                
                case GrpcContentType.Protobuf:
                return DbEntityType.Protobuf;

            }

            throw new Exception("Unsupported content type: " + contentType);
        }
        
        public static GrpcContentType ToGrpcEntityContentType(this DbEntityType  contentType)
        {

            switch (contentType)
            {
                case DbEntityType.Json:
                    return GrpcContentType.Json;
                
                case DbEntityType.Protobuf:
                    return GrpcContentType.Protobuf;

            }

            throw new Exception("Unsupported content type: " + contentType);
        }
        
    }
}