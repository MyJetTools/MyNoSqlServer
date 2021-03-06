using System;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Blob;

namespace MyNoSqlServer.AzureStorage
{

    public class TableMetadata
    {
        public bool Persist { get; set; }

        public static TableMetadata Create(bool persist)
        {
            return new TableMetadata
            {
                Persist = persist
            };
        }

        public static TableMetadata CreateDefault()
        {
            return new TableMetadata
            {
                Persist = true
            };
        }
        
    }
    
    
    public static class TableMetadataSaver
    {


        public static async ValueTask<TableMetadata> GetTableMetadataAsync(CloudBlobContainer container)
        {
            var blobReference = container.GetBlockBlobReference(SystemFileNames.TableMetadataFileName);
            if (!await blobReference.ExistsAsync())
                return TableMetadata.CreateDefault();

            try
            {

                var memoryStream = new MemoryStream();
                await blobReference.DownloadToStreamAsync(memoryStream);
                var bytes = memoryStream.ToArray();
                var json = Encoding.ASCII.GetString(bytes);

                return Newtonsoft.Json.JsonConvert.DeserializeObject<TableMetadata>(json);

            }
            catch (Exception)
            {
                return TableMetadata.CreateDefault();
            }
        }
        
        

        public static async ValueTask SaveTableMetadataAsync(CloudBlobContainer container,  bool persist)
        {
            var metadataModel = TableMetadata.Create(persist);

            var json = Newtonsoft.Json.JsonConvert.SerializeObject(metadataModel);

            var bytes = Encoding.ASCII.GetBytes(json);

            var blobReference = container.GetBlockBlobReference(SystemFileNames.TableMetadataFileName);
            await blobReference.UploadFromByteArrayAsync(bytes, 0, bytes.Length);
        }
        
    }
}