using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Newtonsoft.Json;
using System.Collections.Concurrent;
using System.Text;
using Client.Models;

public class RpcClient : IAsyncDisposable
{
    //On Crée la queue dans la quelle va se trouver les requests des clients
    private const string QUEUE_NAME = "rpc_queue";

    private readonly IConnectionFactory _connectionFactory;
    private readonly ConcurrentDictionary<string, TaskCompletionSource<Response>> _callbackMapper
        = new();
    
    private IConnection? _connection;
    private IChannel? _channel;
    private string? _replyQueueName;

    public RpcClient()
    {
        _connectionFactory = new ConnectionFactory { HostName = "localhost" };
    }

    public async Task StartAsync()
    {
        _connection = await _connectionFactory.CreateConnectionAsync();
        _channel = await _connection.CreateChannelAsync();

        // on déclare la queue ou va se trouver la reponse du serveur.
        QueueDeclareOk queueDeclareResult = await _channel.QueueDeclareAsync();
        // on extrait le nom de la queue
        _replyQueueName = queueDeclareResult.QueueName;
        var consumer = new AsyncEventingBasicConsumer(_channel);

        consumer.ReceivedAsync += (model, ea) =>
        {
            string? correlationId = ea.BasicProperties.CorrelationId;

            if (false == string.IsNullOrEmpty(correlationId))
            {
                if (_callbackMapper.TryRemove(correlationId, out var tcs))
                {
                    var body = ea.Body.ToArray();
                    var response = Encoding.UTF8.GetString(body);
                    Response? res = JsonConvert.DeserializeObject<Response>(response);
                    if (res != null) tcs.TrySetResult(res);
                }
            }

            return Task.CompletedTask;
        };

        await _channel.BasicConsumeAsync(_replyQueueName, true, consumer);
    }

    public async Task<Response> CallAsync(Request message, CancellationToken cancellationToken = default)
    {
        if (_channel is null)
        {
            throw new InvalidOperationException();
        }

        string correlationId = Guid.NewGuid().ToString();
        var props = new BasicProperties
        {
            CorrelationId = correlationId,
            ReplyTo = _replyQueueName
        };

        var tcs = new TaskCompletionSource<Response>(
                TaskCreationOptions.RunContinuationsAsynchronously);
        _callbackMapper.TryAdd(correlationId, tcs);
        
        string serializedMessage = JsonConvert.SerializeObject(message); 
        var messageBytes = Encoding.UTF8.GetBytes(serializedMessage);
        await _channel.BasicPublishAsync(exchange: string.Empty, routingKey: QUEUE_NAME,
            mandatory: true, basicProperties: props, body: messageBytes);

        using CancellationTokenRegistration ctr =
            cancellationToken.Register(() =>
            {
                _callbackMapper.TryRemove(correlationId, out _);
                tcs.SetCanceled();
            });

        return await tcs.Task;
    }

    public async ValueTask DisposeAsync()
    {
        if (_channel is not null)
        {
            await _channel.CloseAsync();
        }

        if (_connection is not null)
        {
            await _connection.CloseAsync();
        }
    }
}

public class Rpc
{
    public static async Task Main(string[] args)
    {
        Console.WriteLine("RPC Client");
        int amount;
        string currency;
        int ecommerceId;
        string customerName;
        Console.WriteLine("Enter amount: ");
        amount = Convert.ToInt32(Console.ReadLine());
        Dictionary<string, string> validCurrencies = new Dictionary<string, string>
        {
            { "USD", "United States Dollar" },
            { "EUR", "Euro" },
            { "QAR", "Qatari Riyal" },
            { "LBP", "Lebanese Pound" }
        };

        while (true){
        Console.WriteLine("Enter currency: ");
        currency = Console.ReadLine().ToUpper();

        if (validCurrencies.ContainsKey(currency))
        {
            Console.WriteLine($"You entered a valid currency: {currency} - {validCurrencies[currency]}");
            break;
        }
        else
        {
            Console.WriteLine("Invalid currency. Please enter a valid currency code (USD, EUR, QAR, LBP).");
        }
        }
        Console.WriteLine("Enter ecommerce ID: ");
        ecommerceId = Convert.ToInt32(Console.ReadLine());
        Console.WriteLine("Enter customer name: ");
        customerName = Console.ReadLine();
        Request request = new Request()
        {
            amount = amount,
            currency = currency,
            ecommerceId = ecommerceId,
            CustomerName = customerName
        };
        
        await InvokeAsync(request);

        Console.WriteLine(" Press [enter] to exit.");
        Console.ReadLine();
    }

    private static async Task InvokeAsync(Request request)
    {
        var rpcClient = new RpcClient();
        await rpcClient.StartAsync();

        Console.WriteLine($"  Sending the following transaction: amount {request.amount}\n  Currency {request.currency}\n  Ecommerce ID {request.ecommerceId}\n  Customer name {request.CustomerName}");
        var response = await rpcClient.CallAsync(request);
        Console.WriteLine(" [.] Got id " + response.id);
    }
}