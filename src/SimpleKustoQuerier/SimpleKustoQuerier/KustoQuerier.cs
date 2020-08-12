using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Dynamic;
using System.Linq;
using System.Threading.Tasks;
using Kusto.Data;
using Kusto.Data.Common;
using Kusto.Data.Net.Client;
using Kusto.Data.Results;
using Microsoft.Azure.Services.AppAuthentication;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace SimpleKustoQuerier
{
    public class KustoQuerier
    {
        private readonly ILogger log;
        private readonly string cluster;
        private readonly string database;

        public KustoQuerier(string cluster, string database)
        {
            this.log = NullLogger<KustoQuerier>.Instance;
            this.cluster = cluster;
            this.database = database;
        }

        public KustoQuerier(ILogger log, string cluster, string database)
        {
            this.log = log;
            this.cluster = cluster;
            this.database = database;
        }

        public async Task<IEnumerable<dynamic>> ExecuteQueryAsync(string query)
        {
            var clientRequestProperties = new ClientRequestProperties() { ClientRequestId = Guid.NewGuid().ToString() };
            return await ExecuteQueryAsync(query, clientRequestProperties);
        }

        public async Task<IEnumerable<dynamic>> ExecuteQueryAsync(string query, KustoConnectionStringBuilder connectionStringBuilder)
        {
            var clientRequestProperties = new ClientRequestProperties() { ClientRequestId = Guid.NewGuid().ToString() };
            return await ExecuteQueryAsync(query, clientRequestProperties, connectionStringBuilder);
        }

        public async Task<IEnumerable<dynamic>> ExecuteQueryAsync(string query, ClientRequestProperties clientRequestProperties)
        {
            var tokenProvider = new AzureServiceTokenProvider(); // Token is cached across instances
            string accessToken = await tokenProvider.GetAccessTokenAsync(cluster);
            var connectionStringBuilder = new KustoConnectionStringBuilder(cluster, database).WithAadApplicationTokenAuthentication(accessToken);
            connectionStringBuilder.FederatedSecurity = true;
            return await ExecuteQueryAsync(query, clientRequestProperties, connectionStringBuilder);
        }

        public async Task<IEnumerable<dynamic>> ExecuteQueryAsync(string query, ClientRequestProperties clientRequestProperties, KustoConnectionStringBuilder connectionStringBuilder)
        {
            using var queryProvider = KustoClientFactory.CreateCslQueryProvider(connectionStringBuilder);
            var progressiveDataSet = await queryProvider.ExecuteQueryV2Async(database, query, clientRequestProperties);
            return LoadQueryResults(progressiveDataSet);
        }

        private IEnumerable<dynamic> LoadQueryResults(ProgressiveDataSet dataset)
        {
            List<dynamic> results = new List<dynamic>();

            foreach (var progressiveData in ProcessProgressiveDataSet(dataset))
            {
                var reader = progressiveData.DataReader;
                var names = Enumerable.Range(0, reader.FieldCount).Select(reader.GetName).ToList();
                foreach (IDataRecord record in (IEnumerable)reader)
                {
                    var expando = new ExpandoObject() as IDictionary<string, object>;
                    foreach (var name in names)
                    {
                        expando[name] = record[name];
                    }

                    results.Add(expando);
                }
            }

            return results;
        }

        private IEnumerable<ProgressiveData> ProcessProgressiveDataSet(ProgressiveDataSet dataset)
        {
            DataTable dataTable = null;
            int currentTableId = -1;
            double completionPercentage = 0;

            using (dataset)
            {
                var enumerator = dataset.GetFrames();
                while (enumerator.MoveNext())
                {
                    switch (enumerator.Current?.FrameType)
                    {
                        case FrameType.DataSetHeader:
                            {
                                // This is a Kusto.Data.Results.ProgressiveDataSetHeaderFrame frame, which is the
                                // first frame in the protocol (contains protocol metadata - just a version for now)
                                var frame = (ProgressiveDataSetHeaderFrame)enumerator.Current;
                                log.LogDebug($"Received FrameType.DataSetHeader. IsProgressive:{frame.IsProgressive}");

                                break;
                            }

                        case FrameType.TableHeader:
                            {
                                // This is a Kusto.Data.Results.ProgressiveDataSetDataTableSchemaFrame frame, which
                                // is the first frame for a new data table in the data set and describes the table
                                // and its schema.
                                HandleTableHeader((ProgressiveDataSetDataTableSchemaFrame)enumerator.Current, ref currentTableId, ref dataTable);
                                break;
                            }

                        case FrameType.TableFragment:
                            {
                                // This is a Kusto.Data.Results.ProgressiveDataSetDataTableFragmentFrame frame,
                                // which includes the data table records.
                                var subTypeHandled = HandleTableFragment((ProgressiveDataSetDataTableFragmentFrame)enumerator.Current, currentTableId, dataTable);
                                if (subTypeHandled != null)
                                {
                                    yield return new ProgressiveData(new DataTableReader2(dataTable), completionPercentage, subTypeHandled == TableFragmentType.DataReplace);
                                }

                                break;
                            }

                        case FrameType.TableCompletion:
                            {
                                // This is a Kusto.Data.Results.ProgressiveDataSetTableCompletionFrame frame, which
                                // is the last frame of the data table.
                                var frame = (ProgressiveDataSetTableCompletionFrame)enumerator.Current;
                                if (currentTableId == frame.TableId)
                                {
                                    Debug.Assert(dataTable != null, "Table should be null by completion frame.");
                                }

                                log.LogDebug($"Received FrameType.TableCompletion. Table RowCount:{frame.RowCount}");
                                currentTableId = -1;
                                break;
                            }

                        case FrameType.TableProgress:
                            {
                                // This is a Kusto.Data.Results.ProgressiveDataSetTableProgressFrame frame, which
                                // informs of the progress made so far in a data table.
                                var frame = (ProgressiveDataSetTableProgressFrame)enumerator.Current;
                                completionPercentage = frame.TableProgress;
                                log.LogDebug($"Received FrameType.TableProgress. CompletionPercentage {completionPercentage:0.##}%");

                                break;
                            }

                        case FrameType.DataTable:
                            {
                                // This is a Kusto.Data.Results.ProgressiveDataSetDataTableFrame frame, which represents
                                // a (usually) small table which includes the entire table schema and data

                                var frame = (ProgressiveDataSetDataTableFrame)enumerator.Current;
                                log.LogDebug($"Received FrameType.DataTable. TableKind:{frame.TableKind}");

                                if (frame.TableKind == WellKnownDataSet.PrimaryResult)
                                {
                                    Debug.Assert(dataTable == null, "Expecting one primary result");
                                    dataTable = new DataTable();
                                    dataTable.Load(frame.TableData);
                                    yield return new ProgressiveData(new DataTableReader2(dataTable), 100, null);
                                }
                                else
                                {
                                    // need to read the data table to advance the enumerator
                                    log.LogDebug($"Ignoring data for TableKind:{frame.TableKind}");
                                    var reader = frame.TableData;
                                    DataTable table = new DataTable();
                                    table.Load(reader);
                                }

                                break;
                            }

                        case FrameType.DataSetCompletion:
                            {
                                // This is a Kusto.Data.Results.ProgressiveDataSetCompletionFrame frame, which is
                                // the last frame of the entire data set.
                                var frame = (ProgressiveDataSetCompletionFrame)enumerator.Current;
                                log.LogDebug("Received FrameType.DataSetCompletion.");
                                if (frame.HasErrors)
                                {
                                    log.LogError("Dataframe has errors.");
                                }
                                else if (frame.Cancelled)
                                {
                                    log.LogWarning("Dataset was cancelled by Kusto.");
                                    throw new OperationCanceledException();
                                }

                                if (frame.Exception != null)
                                {
                                    throw frame.Exception;
                                }

                                break;
                            }

                        default:
                            throw new ArgumentOutOfRangeException();
                    }
                }
            }
        }

        private TableFragmentType? HandleTableFragment(ProgressiveDataSetDataTableFragmentFrame frame, int currentTableId, DataTable dataTable)
        {
            if (currentTableId != frame.TableId)
            {
                return null;
            }

            dataTable.Rows.Clear();
            dataTable.MinimumCapacity = 100;

            object[] values = new object[frame.FieldCount];

            log.LogDebug($"Received ProgressiveDataSetDataTableFragmentFrame. FieldCount:{frame.FieldCount}, FrameSubType:{frame.FrameSubType}, TableId:{frame.TableId}");

            while (frame.GetNextRecord(values))
            {
                dataTable.Rows.Add(values);
            }

            return frame.FrameSubType;
        }

        private void HandleTableHeader(ProgressiveDataSetDataTableSchemaFrame frame, ref int currentTableId, ref DataTable dataTable)
        {
            log.LogDebug($"Received ProgressiveDataSetDataTableSchemaFrame. TableKind:{frame.TableKind.ToString()}, TableId:{frame.TableId}, TableName:{frame.TableName}");

            if (frame.TableKind != WellKnownDataSet.PrimaryResult)
            {
                return;
            }

            currentTableId = frame.TableId;
            Debug.Assert(dataTable == null, "Expecting only one primary result");
            dataTable = frame.ToEmptyDataTable();
        }

        private class ProgressiveData
        {
            public ProgressiveData(IDataReader dataReader, double completionPercentage, bool? replace)
            {
                DataReader = dataReader;
                CompletionPercentage = completionPercentage;
                Replace = replace;
            }

            public IDataReader DataReader { get; private set; }

            /// <summary>
            /// What percentage of results have been returned
            /// </summary>
            // ReSharper disable once UnusedAutoPropertyAccessor.Local
            public double CompletionPercentage { get; private set; }

            /// <summary>
            /// Append: Append this data to previous results
            /// Replace: Delete all previous results for this query and use this as the new result
            /// null: This is the only set.
            /// </summary>
            // ReSharper disable once UnusedAutoPropertyAccessor.Local
            public bool? Replace { get; private set; }
        }
    }
}

