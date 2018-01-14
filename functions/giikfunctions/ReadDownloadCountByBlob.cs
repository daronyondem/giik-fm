using System.Configuration;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using giikfunctions.Models;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using Newtonsoft.Json;

namespace giikfunctions
{
    public static class ReadDownloadCountByBlob
    {
        [FunctionName("ReadDownloadCountByBlob")]
        public static async Task<HttpResponseMessage> Run([HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = null)]HttpRequestMessage req, TraceWriter log)
        {
            log.Info("C# HTTP trigger function for ReadDownloadCountByBlob processed a request.");

            string blobName = req.GetQueryNameValuePairs()
                .FirstOrDefault(q => string.Compare(q.Key, "blob", true) == 0)
                .Value;

            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(ConfigurationManager.AppSettings["AnalyticsStorage"]);

            CloudTableClient tableClient = storageAccount.CreateCloudTableClient();
            CloudTable table = tableClient.GetTableReference("BlobAnalytics");

            TableOperation retrieveOperation = TableOperation.Retrieve<PartitionRowCount>(blobName, "Count");
            TableResult retrievedResult = await table.ExecuteAsync(retrieveOperation).ConfigureAwait(false);
            var countEntity = (PartitionRowCount)retrievedResult.Result;
            if (countEntity == null)
            {
                return req.CreateResponse(HttpStatusCode.NotFound, "Specified blob was not found.");
            }
            else
            {
                var jsonToReturn = JsonConvert.SerializeObject(countEntity);
                return new HttpResponseMessage(HttpStatusCode.OK)
                {
                    Content = new StringContent(jsonToReturn, Encoding.UTF8, "application/json")
                };
            }
        }
    }
}
