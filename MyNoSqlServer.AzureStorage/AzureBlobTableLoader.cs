using System;
using System.Collections.Generic;
using System.IO;
using Microsoft.WindowsAzure.Storage.Blob;
using MyNoSqlServer.Common;
using MyNoSqlServer.Domains.Persistence;

namespace MyNoSqlServer.AzureStorage
{
    public class AzureBlobTableLoader : ITableLoader
    {
        private readonly CloudBlobContainer _cloudBlobContainer;

        public AzureBlobTableLoader(CloudBlobContainer cloudBlobContainer)
        {
            _cloudBlobContainer = cloudBlobContainer;
        }

        public string TableName => _cloudBlobContainer.Name;

        public async IAsyncEnumerable<PartitionSnapshot> LoadSnapshotsAsync()
        {
            await foreach (var blockBlob in _cloudBlobContainer.GetListOfBlobsAsync())
            {
                var memoryStream = new MemoryStream();

                await blockBlob.DownloadToStreamAsync(memoryStream);

                var snapshot = new PartitionSnapshot
                {
                    TableName = _cloudBlobContainer.Name,
                    PartitionKey = blockBlob.Name.Base64ToString(),
                    Snapshot = memoryStream.ToArray()
                };

                yield return snapshot;
                Console.WriteLine("Loaded snapshot: " + snapshot);
            }
        }
    }
}