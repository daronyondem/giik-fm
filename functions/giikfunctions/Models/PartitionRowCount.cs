using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace giikfunctions.Models
{
    class PartitionRowCount : TableEntity
    {
        public int Count { get; set; }
    }
}
