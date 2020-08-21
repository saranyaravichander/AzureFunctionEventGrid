// Default URL for triggering event grid function in the local environment.
// http://localhost:7071/runtime/webhooks/EventGrid?functionName={functionname}
using System;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Azure.EventGrid.Models;
using Microsoft.Azure.WebJobs.Extensions.EventGrid;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System.Globalization;
using Microsoft.WindowsAzure.Storage;
using Avro.File;
using Avro.Generic;
using System.Text;
using Microsoft.Data.SqlClient;
using System.Data;
using System.IO;

namespace EventGridTriggerTest
{
    public static class Function1
    {
        private static readonly string StorageConnectionString = Environment.GetEnvironmentVariable("StorageConnectionString");
        private static readonly string SqlDwConnection = Environment.GetEnvironmentVariable("SqlDwConnection");


        /// <summary>
        /// Use the accompanying .sql script to create this table in the data warehouse
        /// </summary>  
        private const string TableName = "dbo.Fact_WindTurbineMetrics";

        [FunctionName("EventGridTriggerData")]
        public static async System.Threading.Tasks.Task RunAsync([EventGridTrigger] EventGridEvent eventGridEvent, ILogger log)
        {
            log.LogInformation(eventGridEvent.Data.ToString());
            log.LogInformation("C# EventGrid trigger function processed a request.");
            log.LogInformation(eventGridEvent.ToString());


            try
            {
                // Copy to a static Album instance
                EventGridEHEvent ehEvent = new EventGridEHEvent();
                ehEvent.id = eventGridEvent.Id;
                ehEvent.eventTime = eventGridEvent.EventTime.ToString();
                ehEvent.eventType = eventGridEvent.EventType;
                ehEvent.subject = eventGridEvent.Subject;
                ehEvent.topic = eventGridEvent.Topic;
                ehEvent.data = new Data();

                dynamic gridData = eventGridEvent.Data;

                ehEvent.data.fileUrl = gridData.fileUrl;
       
        // Get the URL from the event that points to the Capture file
        var uri = new Uri(ehEvent.data.fileUrl);

                // Get data from the file and migrate to data warehouse
              await  DumpAsync(uri);
            }
            catch (Exception e)
            {
                string s = string.Format(CultureInfo.InvariantCulture,
                    "Error processing request. Exception: {0}, Request: {1}", e.Message + e.StackTrace, eventGridEvent.ToString());
                log.LogError(s);
            }
        }

        /// <summary>
        /// Dumps the data from the Avro blob to the data warehouse (DW). 
        /// Before running this, ensure that the DW has the required <see cref="TableName"/> table created.
        /// </summary>   
        private static async System.Threading.Tasks.Task DumpAsync(Uri fileUri)
        {
            // Get the blob reference
            var storageAccount = CloudStorageAccount.Parse(StorageConnectionString);
            var blobClient = storageAccount.CreateCloudBlobClient();
            var blob = blobClient.GetBlobReferenceFromServerAsync(fileUri).Result;

            using (var dataTable = GetWindTurbineMetricsTable())
            {
                using(var blobStream = new MemoryStream())
                {
                    await blob.DownloadToStreamAsync(blobStream);
                    // Parse the Avro File
                    using (var avroReader = DataFileReader<GenericRecord>.OpenReader(blobStream))
                    {
                        while (avroReader.HasNext())
                        {
                            GenericRecord r = avroReader.Next();

                            byte[] body = (byte[])r["Body"];
                            var windTurbineMeasure = DeserializeToWindTurbineMeasure(body);

                            // Add the row to in memory table
                            AddWindTurbineMetricToTable(dataTable, windTurbineMeasure);
                        }
                    }
                }

                if (dataTable.Rows.Count > 0)
                {
                    BatchInsert(dataTable);
                }
            }
        }

        /// <summary>
        /// Open connection to data warehouse. Write the parsed data to the table. 
        /// </summary>   
        private static void BatchInsert(DataTable table)
        {
            // Write the data to SQL DW using SqlBulkCopy
            using (var sqlDwConnection = new SqlConnection(SqlDwConnection))
            {
                sqlDwConnection.Open();

                using (var bulkCopy = new SqlBulkCopy(sqlDwConnection))
                {
                    bulkCopy.BulkCopyTimeout = 30;
                    bulkCopy.DestinationTableName = TableName;
                    bulkCopy.WriteToServer(table);
                }
            }
        }

        /// <summary>
        /// Deserialize data and return object with expected properties.
        /// </summary> 
        private static WindTurbineMeasure DeserializeToWindTurbineMeasure(byte[] body)
        {
            string payload = Encoding.ASCII.GetString(body);
            return JsonConvert.DeserializeObject<WindTurbineMeasure>(payload);
        }

        /// <summary>
        /// Define the in-memory table to store the data. The columns match the columns in the .sql script.
        /// </summary>   
        private static DataTable GetWindTurbineMetricsTable()
        {
            var dt = new DataTable();
            dt.Columns.AddRange
            (
                new DataColumn[5]
                {
                    new DataColumn("DeviceId", typeof(string)),
                    new DataColumn("MeasureTime", typeof(DateTime)),
                    new DataColumn("GeneratedPower", typeof(float)),
                    new DataColumn("WindSpeed", typeof(float)),
                    new DataColumn("TurbineSpeed", typeof(float))
                }
            );

            return dt;
        }

        /// <summary>
        /// For each parsed record, add a row to the in-memory table.
        /// </summary>  
        private static void AddWindTurbineMetricToTable(DataTable table, WindTurbineMeasure wtm)
        {
            table.Rows.Add(wtm.DeviceId, wtm.MeasureTime, wtm.GeneratedPower, wtm.WindSpeed, wtm.TurbineSpeed);
        }
    }
}
