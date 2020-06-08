using System;
using System.Net;
using System.Net.Http;
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
            return url.SetQueryParam("partitionKey", partitionKey);
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
            return url.SetQueryParam("rowKey", tableName);
        }
        
        
        public static async ValueTask<T> ReadAsJsonAsync<T>(this Task<HttpResponseMessage> responseTask)
        {
            var response = await responseTask;
            return await response.ReadAsJsonAsync<T>();
        }

        public static async ValueTask<T> ReadAsJsonAsync<T>(this HttpResponseMessage response)
        {
            var json = await response.Content.ReadAsStringAsync();
            return Newtonsoft.Json.JsonConvert.DeserializeObject<T>(json);
        }

        internal static async ValueTask<OperationResult> GetOperationResultCodeAsync(
            this HttpResponseMessage httpResponseMessage)
        {
            switch (httpResponseMessage.StatusCode)
            {
                case HttpStatusCode.OK:
                    return OperationResult.Ok;

                case HttpStatusCode.Conflict:
                    var message = await httpResponseMessage.Content.ReadAsStringAsync();
                    return (OperationResult) int.Parse(message);

                default:
                    var messageUnknown = await httpResponseMessage.Content.ReadAsStringAsync();
                    throw new Exception(
                        $"Unknown HTTP result Code{httpResponseMessage.StatusCode}. Message: {messageUnknown}");
            }
        }

    }
}