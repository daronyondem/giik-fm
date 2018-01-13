using Microsoft.WindowsAzure.Storage.Table;

namespace giikfunctions.Models
{
    class BlobAnalytics : TableEntity
    {
        public string RawData { get; set; }
        public string RequestUrl { get; set; }
        public string RequesterIpAddress { get; set; }
        public string UserAgentHeader { get; set; }
        public string Referrer { get; set; }
        public string DownloadTime { get; set; }
    }
}