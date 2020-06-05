using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using MyNoSqlServer.Common;

namespace MyNoSqlServer.AzureStorage
{
    public static class AzureStorageBlobDecorators
    {
        
        private static readonly BlobRequestOptions RequestOptions=
            new BlobRequestOptions
            {
                MaximumExecutionTime = TimeSpan.FromSeconds(10)
            };


        private static CloudBlobContainer GetContainerReference(this CloudStorageAccount storageAccount, string container)
        {
            NameValidator.ValidateContainerName(container);

            var blobClient = storageAccount.CreateCloudBlobClient();
            return blobClient.GetContainerReference(container.ToLower());
        }


        internal static async IAsyncEnumerable<CloudBlobContainer> GetListOfContainersAsync(
            this CloudStorageAccount storageAccount)
        {
            var cloudBlobClient = storageAccount.CreateCloudBlobClient();

            BlobContinuationToken continuationToken = null;

            do
            {
                var response = await cloudBlobClient.ListContainersSegmentedAsync(continuationToken);
                continuationToken = response.ContinuationToken;

                foreach (var result in response.Results)
                    yield return result;

            } while (continuationToken != null);

        }

        internal static async ValueTask<CloudBlobContainer> GetBlockBlobReferenceAsync(this CloudStorageAccount storageAccount, string container)
        {
            var containerRef = storageAccount.GetContainerReference(container);
            
            if (!await containerRef.ExistsAsync())
                await containerRef.CreateIfNotExistsAsync();
            
            var permissions = await containerRef.GetPermissionsAsync(null, RequestOptions, null);
            permissions.PublicAccess = BlobContainerPublicAccessType.Off;
            await containerRef.SetPermissionsAsync(permissions, null, RequestOptions, null);
            
            return containerRef;
        }




        internal static async IAsyncEnumerable<CloudBlob> GetListOfBlobsAsync(
            this CloudBlobContainer cloudBlobContainer)
        {

            var dir = cloudBlobContainer.GetDirectoryReference("");

            BlobContinuationToken continuationToken = null;

            do
            {
                var response = await dir.ListBlobsSegmentedAsync(continuationToken);
                continuationToken = response.ContinuationToken;

                foreach (var blob in response.Results.Cast<CloudBlockBlob>())
                    yield return blob;

            } while (continuationToken != null);

        }

        internal static string PartitionKeyToBlobName(this string partitionKey)
        {
            return partitionKey.ToBase64();
        }

        internal static async Task SavePartitionAsync(this CloudBlobContainer container, string partitionKey, byte[] data)
        {
            var blob = container.GetBlockBlobReference(partitionKey.PartitionKeyToBlobName());
            blob.Properties.ContentType = "application/json";

            await blob.UploadFromByteArrayAsync(data, 0, data.Length);
        }
        
        internal static async Task DeleteBlobAsync(this CloudBlob blob)
        {

            try
            {
                if (await blob.ExistsAsync())
                    await blob.DeleteAsync();
            }
            catch (Exception e)
            {
                Console.WriteLine($"Can not delete blob: {blob.Name}. Reason:{e.Message}");
            }
        }
        
        internal static async Task DeletePartitionAsync(this CloudBlobContainer container, string partitionKey)
        {
            var blob = container.GetBlockBlobReference(partitionKey.PartitionKeyToBlobName());
            await blob.DeleteBlobAsync();
        }
        
        internal static async Task CleanContainerAsync(this CloudBlobContainer container)
        {
            try
            {

                await foreach (var cloudBlob in container.GetListOfBlobsAsync())
                    await cloudBlob.DeleteBlobAsync(); 
                   
            }
            catch (Exception e)
            {
                Console.WriteLine($"Can not clean container: {container.Name}. Reason:{e.Message}");
            }
        }

    }
}