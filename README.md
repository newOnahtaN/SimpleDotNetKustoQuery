![BuildAndPulishNuget](https://github.com/microsoft/SimpleDotNetKustoQuerier/workflows/BuildAndPulishNuget/badge.svg)
# Description
[The first party .NET Kusto client library](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/api/netfx/about-kusto-data) provides a highly abstracted API similar to that of ADO.NET, which is useful for some scenarios but cumbersome if all that is wanted is straightforward query execution. 

This .NET Standard library provides a handful of simple query execution methods which allow it's users to cut through those abstractions and simply execute and receive a list of dynamic objects in response. 

# Examples

Simple Query, assuming implicit AAD service authentication
```
string query = "GenericTableName | limit 10";
var kustoQuerier = new KustoQuerier("clustername", "databasename");
IEnumerable<dynamic> = await kustoQuerier.ExecuteQueryAsync(query);
```

Simple Query, with explicit authentication
```
// Any authentication method provided as a part of KustoConnectionStringBuilder can be used.
// We use AAD Authentication in this example.
// Reference: https://docs.microsoft.com/en-us/azure/data-explorer/kusto/api/netfx/about-kusto-data
var connectionStringBuilder = new KustoConnectionStringBuilder(cluster, database).WithAadApplicationTokenAuthentication(accessToken);
string accessToken = "<access_token>"
connectionStringBuilder.FederatedSecurity = true;

string query = "GenericTableName | limit 10";
var kustoQuerier = new KustoQuerier("clustername", "databasename");
IEnumerable<dynamic> = await kustoQuerier.ExecuteQueryAsync(query, connectionStringBuilder);
```

Simple Query, with explicit authentication and query parameters
```
var clientRequestProperties = new ClientRequestProperties { ClientRequestId = Guid.NewGuid().ToString() };
clientRequestProperties.SetParameter("ExampleParameter", exampleParameterValue);

var connectionStringBuilder = new KustoConnectionStringBuilder(cluster, database).WithAadApplicationTokenAuthentication(accessToken);
string accessToken = "<access_token>"
connectionStringBuilder.FederatedSecurity = true;

string query = "GenericTableName | limit 10";
var kustoQuerier = new KustoQuerier("clustername", "databasename");
IEnumerable<dynamic> = await kustoQuerier.ExecuteQueryAsync(query, clientRequestProperties, connectionStringBuilder);
```


# Contributing

This project welcomes contributions and suggestions.  Most contributions require you to agree to a
Contributor License Agreement (CLA) declaring that you have the right to, and actually do, grant us
the rights to use your contribution. For details, visit https://cla.opensource.microsoft.com.

When you submit a pull request, a CLA bot will automatically determine whether you need to provide
a CLA and decorate the PR appropriately (e.g., status check, comment). Simply follow the instructions
provided by the bot. You will only need to do this once across all repos using our CLA.

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/).
For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or
contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.
