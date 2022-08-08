// See https://aka.ms/new-console-template for more information

using System.ComponentModel.DataAnnotations.Schema;
using Microsoft.Azure.Cosmos;

Console.WriteLine("Azure Cosmos DB Bulk Delete by Query.");

// query for deletion
string query = "SELECT *";

// variables
string _endpointUrl = "";
string _authKey = "";
string _databaseName = "";
string _containerName = "";

Database _database = null;
CosmosClient _client = null;

try
{
    // initialize container
    Container container = await InitializeAsync();

    // populate db
    //await PopulateDatabaseAsync(container);

    // remove all data via sql query
    await RemoveItemsByQueryAsync(container, query);

}
catch (CosmosException cex)
{
    Console.WriteLine(cex.Message);
}
catch (Exception ex)
{
    throw ex;
}

#region Seeding

async Task PopulateDatabaseAsync(Container container)
{

}

#endregion

#region Removal

async Task RemoveItemsByQueryAsync(Container container, string query)
{

}

async Task<Container> InitializeAsync()
{
    _client = GetBulkClientInstance(_endpointUrl, _authKey);
    _database = _client.GetDatabase(_databaseName);
    var container = _database.GetContainer(_containerName);

    try
    {
        await container.ReadContainerAsync();
    }
    catch (Exception ex)
    {
        Console.WriteLine("Error in reading collection: {0}", ex.Message);
        throw;
    }

    Console.WriteLine($"Initialized (Database: {_databaseName}), (Container: {_containerName}) with Bulk enabled CosmosClient");
    return container;
}

CosmosClient GetBulkClientInstance(string endpoint, string authKey)
{
    return new CosmosClient(endpoint, authKey, new CosmosClientOptions() { AllowBulkExecution = true });
}

#endregion
