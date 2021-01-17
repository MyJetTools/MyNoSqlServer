using System;
using System.Net;
using System.Threading.Tasks;
using Flurl;
using Flurl.Http;
using MyNoSqlServer.Abstractions;

namespace MyNoSqlServer.DataWriter
{
    public static class MyNoSqlDataWriterUtils
    {
        public static Url AppendDataSyncPeriod(this Url url, DataSynchronizationPeriod dataSynchronizationPeriod)
        {
            return url.SetQueryParam("syncPeriod", dataSynchronizationPeriod.AsString(null));
        }

        public static Url WithPartitionKeyAsQueryParam(this Url url, string partitionKey)
        {
            return partitionKey == null ? url : url.SetQueryParam("partitionKey", partitionKey);
        }
        
        
        public static Url WithLimitAsQueryParam(this Url url, int limit)
        {
            return limit <= 0 ? url : url.SetQueryParam("limit", limit);
        }
        
        public static Url WithSkipAsQueryParam(this Url url, int skip)
        {
            return skip <= 0 ? url : url.SetQueryParam("skip", skip);
        }
        
        public static Url WithResetExpiresTimeAsQueryParam(this Url url, bool resetExpiresTime)
        {
            if (!resetExpiresTime)
                return url;
            
            return url.SetQueryParam("resetExpiresTime", "1");
        }
        
        
        public static Url WithUpdateExpiresAt(this Url url, DateTime? updateExpiresAt)
        {
            return updateExpiresAt == null 
                ? url 
                : url.SetQueryParam("updateExpiresAt", updateExpiresAt.Value.ToString("s"));
        }
        
        public static Url WithRowKeyAsQueryParam(this Url url, string rowKey)
        {
            return url.SetQueryParam("rowKey", rowKey);
        }


        public static IFlurlRequest AllowNonOkCodes(this Url url)
        {
            return url.AllowHttpStatus(HttpStatusCode.NotFound)
                .AllowHttpStatus(HttpStatusCode.Conflict);
        }

        public static Url WithTableNameAsQueryParam(this Url url, string tableName)
        {
            return url.SetQueryParam("tableName", tableName);
        }
        
        
        public static async ValueTask<T> ReadAsJsonAsync<T>(this Task<IFlurlResponse> responseTask)
        {
            var response = await responseTask;
            return await response.ReadAsJsonAsync<T>();
        }

        public static async ValueTask<T> ReadAsJsonAsync<T>(this IFlurlResponse response)
        {
            var json = await response.GetStringAsync();
            return Newtonsoft.Json.JsonConvert.DeserializeObject<T>(json);
        }

        internal static async ValueTask<OperationResult> GetOperationResultCodeAsync(
            this IFlurlResponse httpResponseMessage)
        {
            switch (httpResponseMessage.StatusCode)
            {
                case (int)HttpStatusCode.OK:
                    return OperationResult.Ok;

                case (int)HttpStatusCode.Conflict:
                    var message = await httpResponseMessage.GetStringAsync();
                    return (OperationResult) int.Parse(message);

                default:
                    var messageUnknown = await httpResponseMessage.GetStringAsync();
                    throw new Exception(
                        $"Unknown HTTP result Code{httpResponseMessage.StatusCode}. Message: {messageUnknown}");
            }
        }

    }
}