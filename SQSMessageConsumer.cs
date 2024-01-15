using System;
using System.Threading;
using System.Threading.Tasks;
using Amazon;
using Amazon.Runtime;
using Amazon.SQS;
using Amazon.SQS.Model;
using Amazon.Auth;
using OpenTelemetry;
//using OpenTelemetry.Contrib.Extensions.AWSXRay.Trace;
using OpenTelemetry.Trace;
using OpenTelemetry.Context.Propagation;
using System.ComponentModel;

namespace SQSConsumer
{
    public class SQSMessageConsumer
    {
        private AmazonSQSClient _sqsClient;
        private bool _isPolling;
        private int _delay;
        private string? _queueUrl;
        private string? _awsregion;
        private int _maxNumberOfMessages;
        private int _messageWaitTimeSeconds;
        private string? _accessKey;
        private string? _secret;
        private CancellationTokenSource _source;
        private CancellationToken _token;

        public SQSMessageConsumer()
        {
            try
            {
                _accessKey = Environment.GetEnvironmentVariable("AWS_ACCESS_KEY");
                _secret = Environment.GetEnvironmentVariable("AWS_SECRET");
                _queueUrl = "https://sqs.us-east-1.amazonaws.com/537309256512/BankQueueFinal";//Environment.GetEnvironmentVariable("AWS_QUEUE_URL");;
                _awsregion = "us-east-1";//Environment.GetEnvironmentVariable("AWS_REGION");
                _messageWaitTimeSeconds = 20;
                _maxNumberOfMessages = 10;
                _delay = 0;
            }
            catch (NullReferenceException e)
            {
                Console.WriteLine("AWS credentials not set up through environment variables. Please fix that and re execute.");
                Environment.Exit(1);
            }


            //BasicSessionCredentials basicCredentials = new BasicSessionCredentials(_accessKey, _secret, _token);
            RegionEndpoint region = RegionEndpoint.GetBySystemName(_awsregion);

            _sqsClient = new AmazonSQSClient();
            Console.CancelKeyPress += new ConsoleCancelEventHandler(CancelKeyPressHandler);

        }

        private CompositeTextMapPropagator propagator = new CompositeTextMapPropagator(new TextMapPropagator[] {
            new TraceContextPropagator(),
            //new BaggagePropagator(),
        });
        //private static readonly Func<Dictionary<string, string>, string, IEnumerable<string>> valueGetter = (attributes, name) => attributes.Where(attribute => attribute.Key == name);
        /*private static readonly Func<Dictionary<string, string>, string, IEnumerable<string>> valueGetter =
            (attributes, name) => new List<string>() { 
                KeyValuePair<string, MessageAttributeValue> messageAttribute = attributes[name];
            };*/

        //     private static readonly Func<Dictionary<string, MessageAttributeValue>, string, IEnumerable<string>> valueGetter = (attributes, name) =>
        //     {
        //         new List<string>() {
        //             attributes.Values;
        //     };
        // };

        // public static IEnumerable<string> ValueGetter(Dictionary<string, MessageAttributeValue> carrier, string name)
        // {
        //     return carrier.Values.GetEnumerator();
        // }



        private static readonly Func<Dictionary<string, MessageAttributeValue>, string, IEnumerable<string>> valueGetter = (attributes, name) => new List<string>() {
            attributes[name].StringValue };

        /*private static readonly Func<Dictionary<string, string>, string, IEnumerable<string>> valueGetter = (attributes, name) => new List<string>() { 
                KeyValuePair<string, MessageAttributeValue> messageAttribute = attributes[name];
            };*/

        public async Task Listen()
        {
            _isPolling = true;

            int i = 0;
            try
            {
                _source = new CancellationTokenSource();
                _token = _source.Token;

                while (_isPolling)
                {
                    i++;
                    Console.Write(i + ": ");
                    await FetchFromQueue();
                    Thread.Sleep(_delay);
                }
            }
            catch (TaskCanceledException ex)
            {
                Console.WriteLine("Application Terminated: " + ex.Message);
            }
            finally
            {
                _source.Dispose();
            }
        }

        private async Task FetchFromQueue()
        {

            ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest();
            List<string> AttributesList = new List<string>();
            AttributesList.Add("traceparent");
            AttributesList.Add("tracestate");
            AttributesList.Add("x-dynatrace");
            receiveMessageRequest.AttributeNames = AttributesList;
            receiveMessageRequest.MessageAttributeNames = AttributesList;

            receiveMessageRequest.QueueUrl = _queueUrl;
            receiveMessageRequest.MaxNumberOfMessages = _maxNumberOfMessages;
            receiveMessageRequest.WaitTimeSeconds = _messageWaitTimeSeconds;
            ReceiveMessageResponse receiveMessageResponse = await _sqsClient.ReceiveMessageAsync(receiveMessageRequest, _token);

            if (receiveMessageResponse.Messages.Count != 0)
            {
                for (int i = 0; i < receiveMessageResponse.Messages.Count; i++)
                {
                    string messageBody = receiveMessageResponse.Messages[i].Body;
                    Message message = receiveMessageResponse.Messages[i];
                    Dictionary<string, MessageAttributeValue> messageAttributes = receiveMessageResponse.Messages[i].MessageAttributes;
                    Console.WriteLine("Message attribs:" + String.Join(" - ", messageAttributes.ToArray()));


                    //Console.WriteLine(receiveMessageResponse.Messages[i].Attributes);
                    //Console.WriteLine(receiveMessageResponse.Messages[i].MessageAttributes);
                    // foreach(KeyValuePair<String, String> attribute in receiveMessageResponse.Messages[i].Attributes){
                    //     Console.WriteLine(attribute);
                    //     Console.WriteLine(attribute.Key+": "+attribute.Value);
                    // }

                    

                    foreach (KeyValuePair<String, Amazon.SQS.Model.MessageAttributeValue> attribute in receiveMessageResponse.Messages[i].MessageAttributes)
                    {

                        Console.WriteLine(attribute.Key + ": " + attribute.Value + " -- " + attribute.Value.StringValue);
                    }



                    var parentContext = propagator.Extract(default, receiveMessageResponse.Messages[i].MessageAttributes, SQSMessageConsumer.valueGetter);
                    Console.WriteLine("Parent context: "+parentContext.ToString());
                    
                    //using var activity = MyActivitySource.StartActivity("my-span", ActivityKind.Consumer, parentContext.ActivityContext);
                    Console.WriteLine("Message Received: " + messageBody);

                    await DeleteMessageAsync(receiveMessageResponse.Messages[i].ReceiptHandle);
                }
            }
            else
            {
                Console.WriteLine("No Messages to process");
            }
        }

        private async Task DeleteMessageAsync(string recieptHandle)
        {

            DeleteMessageRequest deleteMessageRequest = new DeleteMessageRequest();
            deleteMessageRequest.QueueUrl = _queueUrl;
            deleteMessageRequest.ReceiptHandle = recieptHandle;

            DeleteMessageResponse response = await _sqsClient.DeleteMessageAsync(deleteMessageRequest);

        }

        protected void CancelKeyPressHandler(object sender, ConsoleCancelEventArgs args)
        {
            args.Cancel = true;
            _source.Cancel();
            _isPolling = false;
        }

    }
}