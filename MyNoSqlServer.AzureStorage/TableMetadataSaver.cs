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
        
        public int MaxPartitionsAmount { get; set; }

        public static TableMetadata Create(bool persist, int maxPartitionsAmount)
        {
            return new ()
            {
                Persist = persist,
                MaxPartitionsAmount = maxPartitionsAmount
            };
        }

        public static TableMetadata CreateDefault()
        {
            return new ()
            {
                Persist = true,
                MaxPartitionsAmount = 0
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
        
        

        public static async ValueTask SaveTableMetadataAsync(CloudBlobContainer container,  bool persist, int maxPartitionsAmount)
        {
            var metadataModel = TableMetadata.Create(persist, maxPartitionsAmount);

            var json = Newtonsoft.Json.JsonConvert.SerializeObject(metadataModel);

            var bytes = Encoding.ASCII.GetBytes(json);

            var blobReference = container.GetBlockBlobReference(SystemFileNames.TableMetadataFileName);
            await blobReference.UploadFromByteArrayAsync(bytes, 0, bytes.Length);
        }
        
    }
}