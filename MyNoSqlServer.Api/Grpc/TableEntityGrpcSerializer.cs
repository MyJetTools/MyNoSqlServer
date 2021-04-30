using MyNoSqlServer.Domains.Db.Rows;
using MyNoSqlServer.Grpc;

namespace MyNoSqlServer.Api.Grpc
{
    internal static class TableEntityGrpcSerializer
    {

        internal static TableEntityTransportGrpcContract ToTransportContract(this DbRow dbRow)
        {

            if (dbRow == null)
                return null;
            
            return new TableEntityTransportGrpcContract
            {
                ContentType = GrpcContentType.Json,
                Content = dbRow.Data
            };
        }


        
    }
}