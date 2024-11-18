using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;
using Newtonsoft.Json;
using Server.Models;

const string QUEUE_NAME = "rpc_queue";

var factory = new ConnectionFactory { HostName = "localhost" };
using var connection = await factory.CreateConnectionAsync();
using var channel = await connection.CreateChannelAsync();
Random rand = new Random();
await channel.QueueDeclareAsync(queue: QUEUE_NAME, durable: false, exclusive: false,
    autoDelete: false, arguments: null);

await channel.BasicQosAsync(prefetchSize: 0, prefetchCount: 1, global: false);

var consumer = new AsyncEventingBasicConsumer(channel);
consumer.ReceivedAsync += async (object sender, BasicDeliverEventArgs ea) =>
{
    AsyncEventingBasicConsumer cons = (AsyncEventingBasicConsumer)sender;
    IChannel ch = cons.Channel;
    Response response = null;

    byte[] body = ea.Body.ToArray();
    IReadOnlyBasicProperties props = ea.BasicProperties;
    var replyProps = new BasicProperties
    {
        CorrelationId = props.CorrelationId
    };

    try
    {
        var message = Encoding.UTF8.GetString(body);
        Request? request = JsonConvert.DeserializeObject<Request>(message);
        Console.WriteLine($" Processing Request of amount {request.amount} for the customer {request.CustomerName}");
        response = new Response()
        {
            id= rand.Next(1,1000000)
        };
    }
    catch (Exception e)
    {
        Console.WriteLine($" [.] {e.Message}");
        
    }
    finally
    {
        var message = JsonConvert.SerializeObject(response);
        var responseBytes = Encoding.UTF8.GetBytes(message);
        await ch.BasicPublishAsync(exchange: string.Empty, routingKey: props.ReplyTo!,
            mandatory: true, basicProperties: replyProps, body: responseBytes);
        await ch.BasicAckAsync(deliveryTag: ea.DeliveryTag, multiple: false);
    }
};

await channel.BasicConsumeAsync(QUEUE_NAME, false, consumer);
Console.WriteLine(" [x] Awaiting RPC requests");
Console.WriteLine(" Press [enter] to exit.");
Console.ReadLine();


