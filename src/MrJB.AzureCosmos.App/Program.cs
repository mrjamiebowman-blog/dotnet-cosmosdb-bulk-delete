// See https://aka.ms/new-console-template for more information

using System.ComponentModel.DataAnnotations.Schema;
using System.Diagnostics;
using System.Net;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json.Linq;

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
    Console.WriteLine($"Starting data removal process.");

    // count
    int count = 1;

    using FeedIterator<JObject> queryOfItems = container.GetItemQueryIterator<JObject>(
        query,
        requestOptions: new QueryRequestOptions()
        {
            MaxBufferedItemCount = 0,
            MaxConcurrency = 1,
            MaxItemCount = 100
        }
    );

    Stopwatch sw = new Stopwatch();
    sw.Start();

    while (queryOfItems.HasMoreResults)
    {
        List<Task> tasks = new List<Task>();

        FeedResponse<JObject> items = await queryOfItems.ReadNextAsync();

        foreach (JObject item in items)
        {
            tasks.Add(RemoveItem(container, item, count));
            count++;
        }

        await Task.WhenAll(tasks);
    }

    // stop 
    sw.Stop();
    Console.WriteLine($"Time: {sw.Elapsed.ToString("mm\\:ss\\.ff")}");
    Console.WriteLine($"Items Removed: {count.ToString()}.");
}

async Task RemoveItem(Container container, JObject item, int count)
{
    ItemRequestOptions itemRequestOptions = new ItemRequestOptions() {
        EnableContentResponseOnWrite = false
    };

    // loop
    while (true)
    {
        string id = item["id"].Value<string>();

        // partition key
        string? pk = item["pk"] != null ? item["pk"].Value<string>() : null;

        itemRequestOptions.IfMatchEtag = item["_etag"].Value<string>();

        try
        {
            // remove
            await container.DeleteItemAsync<JObject>(id, new PartitionKey(pk), itemRequestOptions);
            return;
        }
        catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
        {
            Console.WriteLine($"Not Found (404) with id:{id}, pk:{pk}.");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Exception: {ex.Message}.");
        }
    }
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
