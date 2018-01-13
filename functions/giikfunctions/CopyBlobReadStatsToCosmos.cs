using giikfunctions.Models;
using KBCsv;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Configuration;

namespace giikfunctions
{
    public static class CopyBlobReadStatsToCosmos
    {
        [FunctionName("CopyBlobReadStatsToCosmos")]
        public static void Run([TimerTrigger("0 * * * * *")]TimerInfo myTimer, TraceWriter log)
        {
            log.Info($"Timer trigger function for CopyBlobReadStatsToCosmos executed at: {DateTime.Now}");

            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(ConfigurationManager.AppSettings["AnalyticsStorage"]);
            CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();
            CloudBlobContainer container = blobClient.GetContainerReference("$logs");

            CloudTableClient tableClient = storageAccount.CreateCloudTableClient();
            CloudTable table = tableClient.GetTableReference("BlobAnalytics");

            if (container.Exists())
            {
                foreach (CloudBlob analyticsBlob in container.ListBlobs(useFlatBlobListing:true))
                {
                    if(!analyticsBlob.Uri.ToString().Contains(DateTime.Now.ToString("yyyy/MM/dd")))
                    {
                        var blobAnalyticsList = new List<BlobAnalytics>();

                        System.IO.Stream file = analyticsBlob.OpenRead();
                        var blobReader = new System.IO.StreamReader(file);
                        using (var csvReader = new CsvReader(blobReader))
                        {
                            csvReader.ReadHeaderRecord();

                            while (csvReader.HasMoreRecords)
                            {
                                csvReader.ValueSeparator = ';';
                                var record = csvReader.ReadDataRecord();

                                if (record[2].Trim() == "GetBlob" && record[7].Trim() == "anonymous" && record[20].Trim() != "0" && record[12].Contains(ConfigurationManager.AppSettings["PathToTrack"]))
                                {
                                    var analytics = new BlobAnalytics()
                                    {
                                        RequestUrl = record[11],
                                        RequesterIpAddress = record[15],
                                        PartitionKey = record[12].Replace(ConfigurationManager.AppSettings["PathToTrack"], ""),
                                        UserAgentHeader = record[27],
                                        Referrer = record[28],
                                        RawData = record.ToString(),
                                        DownloadTime = record[1],
                                        RowKey = record[13]
                                    };
                                    blobAnalyticsList.Add(analytics);

                                    var batchOperation = new TableBatchOperation();
                                    batchOperation.Insert(analytics);

                                    TableOperation retrieveOperation = TableOperation.Retrieve<PartitionRowCount>(analytics.PartitionKey, "Count");
                                    TableResult retrievedResult = table.Execute(retrieveOperation);
                                    var updateEntity = (PartitionRowCount)retrievedResult.Result;
                                    if (updateEntity == null)
                                    {
                                        updateEntity = new PartitionRowCount
                                        {
                                            PartitionKey = analytics.PartitionKey,
                                            RowKey = "Count"
                                        };
                                        batchOperation.Insert(updateEntity);
                                    }
                                    else
                                    {
                                        updateEntity.Count++;
                                        batchOperation.Replace(updateEntity);
                                    }

                                    try
                                    {
                                        table.ExecuteBatch(batchOperation);
                                    }
                                    catch (StorageException ex)
                                    {
                                        if (ex.RequestInformation.HttpStatusCode != 409)
                                        {
                                            log.Info($"Exception for {analyticsBlob.Uri}: {ex}");
                                            throw;
                                        }
                                    }
                                }
                            }
                        }
                        file.Close();
                        analyticsBlob.Delete();
                    }
                }
            }
        }
    }
}
